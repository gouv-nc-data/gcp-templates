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


def upload_table(spark: SparkSession, client: bigquery.Client, schema: str, table_name: str, url: str, dataset: str, mode: str):
    get_logger(spark).info("test! %s" % table_name.__class__.__name__)
    get_logger(spark).info("migration table %s" % table_name)
    df = spark.read.jdbc(url, "%s.%s" % (schema, table_name['TABLE_NAME']), properties={"driver": "oracle.jdbc.driver.OracleDriver",
                                                                                        "fetchsize": "10000"})
    # get_logger(spark).info("###############################################")
    get_logger(spark).info(df.dtypes)
    for c_name, c_type in df.dtypes:
        if c_type.startswith('decimal'):
            get_logger(spark).info("conversion de decimal vers float de la colonne %s" % c_name)
            df = df.withColumn(c_name, df[c_name].cast("float"))
            
    get_logger(spark).info("upload de la table %s" % table_name)

    table_id = "%s.%s" % (dataset, table_name['TABLE_NAME'])
    previous_modified = None
    if mode == "overwrite":
        sync_target_schema(spark, client, table_id, df)
        previous_modified = get_table_modified(client, table_id)

    df.write \
        .format("bigquery") \
        .option("writeMethod", "direct") \
        .mode(mode) \
        .save(table_id)

    if mode == "overwrite":
        check_table_written(client, table_id, previous_modified)


def query_factory(spark: SparkSession, schema: str, exclude: str = None) -> str:
    get_logger(spark).info("liste des tables exclues du transfert : '%s'" % exclude)
    if exclude != "":
        query = "SELECT table_name FROM all_tables where owner = '%s' and table_name not in (%s)" % (schema, exclude)
    else:
        query = "SELECT table_name FROM all_tables where owner = '%s'" % schema
    return query


def run(spark: SparkSession, app_name: Optional[str], schema: str, url: str, dataset: str, mode: str, exclude: str):
    query = query_factory(spark, schema, exclude)

    table_names = spark.read \
                       .format("jdbc") \
                       .option("url", url) \
                       .option("driver", "oracle.jdbc.driver.OracleDriver") \
                       .option("query", query) \
                       .option("TimeStampFormat", "dd-MM-yyyy HH:mm:ss") \
                       .option("TreatEmptyValuesAsNulls", True) \
                       .option("IgnoreLeadingWhiteSpace", True) \
                       .option("IgnoreTrailingWhiteSpace", True) \
                       .load()
    get_logger(spark).info("migration de %s tables" % table_names.count())
    get_logger(spark).info(table_names.show())
    client = bigquery.Client()
    failed_tables = []
    for table_name in table_names.collect():
        try:
            upload_table(spark, client, schema, table_name, url, dataset, mode)
        except Exception as e:
            get_logger(spark).error(
                "échec de la migration de la table %s : %s" % (table_name['TABLE_NAME'], e))
            failed_tables.append(table_name['TABLE_NAME'])

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
    
    # parser.add_argument(
    #     '--gcs-bucket',
    #     type=str,
    #     dest='gcs_bucket',
    #     required=True,
    #     help='nom du bucket pour le stockage des fichiers intermédiaires')

    known_args, pipeline_args = parser.parse_known_args()

    spark = SparkSession.builder \
        .appName("Oracle Migration with PySpark") \
        .getOrCreate()
    spark.conf.set("spark.sql.debug.maxToStringFields", 1000)

    run(app_name="database transfert",
        spark=spark,
        schema=known_args.schema,
        url="jdbc:%s" % known_args.jdbc_url,
        dataset=known_args.dataset,
        mode=known_args.mode,
        exclude=known_args.exclude)
