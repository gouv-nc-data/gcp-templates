from pyspark.sql import SparkSession
import argparse
from typing import Optional
from logging import Logger
import time
from google.cloud import bigquery
from google.api_core.exceptions import NotFound


def get_logger(spark: SparkSession) -> Logger:
    """
    Convenience method to get the Spark logger from a SparkSession

    Args:
        spark (SparkSession): The initialized SparkSession object

    Returns:
        Logger: The Spark logger
    """

    log_4j_logger = spark.sparkContext._jvm.org.apache.log4j  # pylint: disable=protected-access
    return log_4j_logger.LogManager.getLogger(__name__)

def create_dataset_if_not_exists(spark: SparkSession, project_id: str, dataset_id: str, location: str = "EU"):
    """
    Crée le dataset BigQuery s'il n'existe pas.
    """
    client = bigquery.Client()
    dataset_ref = f"{project_id}.{dataset_id}"
    
    try:
        client.get_dataset(dataset_ref)
        get_logger(spark).info(f"Le dataset {dataset_ref} existe déjà.")
    except NotFound:
        get_logger(spark).info(f"Le dataset {dataset_ref} n'a pas été trouvé. Création en cours...")
        dataset = bigquery.Dataset(dataset_ref)
        dataset.location = location
        client.create_dataset(dataset, timeout=30)
        get_logger(spark).info(f"Dataset {dataset_ref} créé avec succès dans la localisation {location}.")
    except Exception as e:
        get_logger(spark).error(f"Erreur lors de la vérification/création du dataset : {e}")
        # On ne bloque pas forcément l'exécution car Spark pourrait échouer plus tard si nécessaire

def get_table_size_bytes(spark: SparkSession, url: str, table_name: str) -> int:
    """
    Exécute une requête sur PostgreSQL pour obtenir la taille totale d'une table en octets.
    """
    # Requête pour obtenir la taille de la table dans PostgreSQL
    query = f"(SELECT pg_total_relation_size('{table_name}'))"
    get_logger(spark).info("Requête pour obtenir la taille de la table: %s" % query)
    try:
        size_df = spark.read.jdbc(url, query, properties={"driver": "org.postgresql.Driver"})
        # get_logger(spark).info("dataframe de taille de table: %s" % size_df.show())
        first_row = size_df.first()
        if first_row and len(first_row) > 0:
            size_bytes = first_row[0]
            # size_bytes = size_df.first()['size']
        get_logger(spark).info(f"Taille estimée pour la table {table_name}: {size_bytes / 1e6:.2f} MB")
        return size_bytes if size_bytes else 0
    except Exception as e:
        get_logger(spark).warning(f"Impossible d'estimer la taille de la table {table_name}: {e}. Utilisation d'une valeur par défaut.")
        return 0 # Retourne 0 en cas d'erreur

def schema_delta(bq_columns, source_columns):
    """
    Compare les colonnes d'une table BigQuery et celles de la source.

    Returns:
        tuple: (colonnes ajoutées côté source, colonnes disparues de la source)
    """
    bq_set = {c.lower() for c in bq_columns}
    source_set = {c.lower() for c in source_columns}
    return sorted(source_set - bq_set), sorted(bq_set - source_set)


def sync_target_schema(spark: SparkSession, client: bigquery.Client, table_id: str, df):
    """
    Supprime la table BigQuery si son schéma ne correspond plus à celui de la source.

    Le writeMethod "direct" réutilise le schéma de la table existante : une colonne
    ajoutée côté PostgreSQL fait échouer l'écriture avec
    "Inserted row has wrong column count", et le connecteur se contente d'un WARN.
    Le mode "overwrite" recharge la table entièrement à chaque run : la supprimer
    laisse le connecteur la recréer avec le schéma courant, sans perte de données.
    """
    try:
        table = client.get_table(table_id)
    except NotFound:
        return

    added, removed = schema_delta([field.name for field in table.schema], df.columns)
    if added or removed:
        get_logger(spark).info(
            "schéma modifié pour %s (colonnes ajoutées: %s, supprimées: %s), suppression de la table pour recréation"
            % (table_id, added, removed))
        client.delete_table(table_id)


def get_table_modified(client: bigquery.Client, table_id: str):
    """
    Date de dernière modification de la table BigQuery, None si elle n'existe pas.
    """
    try:
        return client.get_table(table_id).modified
    except NotFound:
        return None


def check_table_written(client: bigquery.Client, table_id: str, previous_modified,
                        attempts: int = 3, delay_seconds: int = 5):
    """
    Vérifie que l'écriture a bien été appliquée à la table BigQuery.

    Le connecteur spark-bigquery avale les erreurs d'écriture (WARN
    "unexpected issue trying to save" puis writer "aborted") sans les remonter à
    PySpark : sans ce contrôle, une table peut rester périmée alors que le batch
    se termine en SUCCEEDED. Une écriture abandonnée laisse la table inchangée,
    donc sa date de modification n'avance pas.

    On compare des dates plutôt que des nombres de lignes : la source reste en
    écriture pendant la migration, un écart de comptage serait normal.
    """
    for attempt in range(attempts):
        modified = get_table_modified(client, table_id)
        if modified is not None and (previous_modified is None or modified > previous_modified):
            return
        if attempt < attempts - 1:
            time.sleep(delay_seconds)

    raise RuntimeError(
        "table %s : écriture non appliquée (date de modification inchangée : %s)"
        % (table_id, previous_modified))


def upload_table(spark: SparkSession, client: bigquery.Client, table_name: str, url: str, dataset: str, mode: str, bucket: str):
    get_logger(spark).info("migration table %s" % table_name['table_name'])
    start_time = time.time()



    df = spark.read.jdbc(url, table_name['table_name'], properties={"driver": "org.postgresql.Driver"})
    elapsed_time = time.time() - start_time
    get_logger(spark).info(f"Table {table_name['table_name']} chargée en {elapsed_time:.2f} secondes.")
    
    get_logger(spark).info(f"Nombre de lignes dans la table {table_name['table_name']}: {df.count()}")
    # get_logger(spark).info(f"Schéma de la table {table_name['table_name']}: {df.dtypes}")
    # try:
    #     get_logger(spark).info(f"Premières lignes de la table {table_name['table_name']}: {df.head(5)}")
    # except Exception as e:
    #     get_logger(spark).warning(f"Impossible d'afficher les premières lignes de la table {table_name['table_name']}: {e}")

    for c_name, c_type in df.dtypes:
        if c_type.startswith('decimal'):
            get_logger(spark).info("conversion de decimal vers float de la colonne %s" % c_name)
            df = df.withColumn(c_name, df[c_name].cast("float"))

    # --- Partitionnement DYNAMIQUE ---
    TARGET_PARTITION_SIZE_BYTES = 9 * 1024 * 1024  # 9 MB max si 10
    total_size_bytes = get_table_size_bytes(spark, url, table_name['table_name'])

    if total_size_bytes > 0:
        numerator = total_size_bytes
        denominator = TARGET_PARTITION_SIZE_BYTES
        num_partitions = (numerator + denominator - 1) // denominator  # Utilisation de la division entière pour arrondir vers le haut

        num_partitions = max(1, num_partitions)
        get_logger(spark).info(f"Nombre de partitions calculé pour {table_name}: {num_partitions}")

        df = df.repartition(num_partitions)  

    get_logger(spark).info("upload de la table %s" % table_name['table_name'])

    table_id = "%s.%s" % (dataset, table_name['table_name'])
    previous_modified = None
    if mode == "overwrite":
        sync_target_schema(spark, client, table_id, df)
        previous_modified = get_table_modified(client, table_id)

    start_time = time.time()
    if len(bucket) > 0:
        df.write \
            .format("bigquery") \
            .option("temporaryGcsBucket", bucket) \
            .mode(mode) \
            .save(table_id)
    else:
        df.write \
            .format("bigquery") \
            .option("writeMethod", "direct") \
            .option("allowFieldAddition", "true") \
            .option("allowFieldRelaxation", "true") \
            .mode(mode) \
            .save(table_id)
    elapsed_time = time.time() - start_time
    get_logger(spark).info(f"Table {table_name['table_name']} uploadée en {elapsed_time:.2f} secondes.")

    if mode == "overwrite":
        check_table_written(client, table_id, previous_modified)

def query_factory(schema: str, exclude: str = None, only: str = None) -> str:
    if exclude != "":
        query = "SELECT table_name FROM information_schema.tables where table_schema = '%s' and table_name not in (%s)" % (schema, exclude)
    else:
        query = "SELECT table_name FROM information_schema.tables where table_schema = '%s'" % schema
    if only != "":
        query += " and table_name in (%s)" % only
    return query


def run(spark: SparkSession, app_name: Optional[str], schema: str, url: str, dataset: str, mode: str, exclude: str, only: str, bucket: str):
    query = query_factory(schema, exclude, only)
    get_logger(spark).info("liste des tables : %s" % query)
    table_names = spark.read \
                       .format("jdbc") \
                       .option("url", url) \
                       .option("driver", "org.postgresql.Driver") \
                       .option("query", query) \
                       .option("TimeStampFormat", "dd-MM-yyyy HH:mm:ss") \
                       .option("TreatEmptyValuesAsNulls", True) \
                       .option("IgnoreLeadingWhiteSpace", True) \
                       .option("IgnoreTrailingWhiteSpace", True) \
                       .load()

    get_logger(spark).info("migration de %s tables" % table_names.count())

    client = bigquery.Client()
    failed_tables = []
    for table_name in table_names.collect():
        try:
            upload_table(spark, client, table_name, url, dataset, mode, bucket)
        except Exception as e:
            get_logger(spark).error(
                "échec de la migration de la table %s : %s" % (table_name['table_name'], e))
            failed_tables.append(table_name['table_name'])

    if failed_tables:
        raise RuntimeError("échec de la migration de %d table(s) : %s"
                           % (len(failed_tables), ", ".join(failed_tables)))

    get_logger(spark).info("fin migration")


if __name__ == '__main__':
    parser = argparse.ArgumentParser()

    parser.add_argument(
        '--jdbc-url',
        type=str,
        dest='jdbc_url',
        required=True,
        help='URL JDBC vers la bdd source')

    parser.add_argument(
        '--schema',
        type=str,
        dest='schema',
        required=True,
        help='schéma à migrer')

    parser.add_argument(
        '--dataset',
        type=str,
        dest='dataset',
        required=True,
        help='schéma à migrer')

    parser.add_argument(
        '--mode',
        type=str,
        dest='mode',
        required=False,
        default="overwrite",
        help='schéma à migrer')

    parser.add_argument(
        '--exclude',
        type=str,
        dest='exclude',
        required=False,
        default="",
        help='tables à exclure de la migration')
    
    parser.add_argument(
        '--only',
        '--only-tables',
        type=str,
        dest='only',
        required=False,
        default="",
        help='tables à inclure dans la migration (alias --only-tables supporté)')

    parser.add_argument(
        '--bucket',
        type=str,
        dest='bucket',
        required=False,
        default="",
        help='bucket temporaire')

    known_args, pipeline_args = parser.parse_known_args()

    spark = SparkSession.builder \
        .appName("PostgreSQL Migration with PySpark") \
        .config("spark.network.timeout", "600s") \
        .getOrCreate()
    spark.conf.set("spark.sql.debug.maxToStringFields", 1000)

    input_url = known_args.jdbc_url
    if input_url[:5] != "jdbc:":
        input_url = "jdbc:%s" % known_args.jdbc_url
    
    # Création du dataset si nécessaire avant de lancer Spark
    # On utilise le client BigQuery pour s'assurer que le dataset existe
    client = bigquery.Client()
    dataset_parts = known_args.dataset.split('.')
    if len(dataset_parts) > 1:
        project_id = dataset_parts[0]
        dataset_id = dataset_parts[1]
    else:
        # Si pas de point, on utilise le projet configuré par défaut pour le client
        project_id = client.project
        dataset_id = known_args.dataset

    create_dataset_if_not_exists(spark, project_id, dataset_id)

    if known_args.only != "":
        get_logger(spark).info("only est défini, exclusion ignorée")
        known_args.exclude = ""

    run(app_name="database transfert",
        spark=spark,
        schema=known_args.schema,
        url=input_url,
        dataset=known_args.dataset,
        mode=known_args.mode,
        exclude=known_args.exclude,
        only=known_args.only,
        bucket=known_args.bucket)
