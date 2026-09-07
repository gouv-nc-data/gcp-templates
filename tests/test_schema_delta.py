from postgresql_to_bigquery import schema_delta
from oracle_to_bigquery import schema_delta as oracle_schema_delta


def test_identical_schemas_should_have_no_delta():
    assert schema_delta(["id", "login"], ["id", "login"]) == ([], [])


def test_case_should_be_ignored():
    assert schema_delta(["ID", "Login"], ["id", "login"]) == ([], [])


def test_added_column_should_be_detected():
    assert schema_delta(["id", "login"], ["id", "login", "mfa_policy"]) == (["mfa_policy"], [])


def test_removed_column_should_be_detected():
    assert schema_delta(["id", "login", "sphinx_password"], ["id", "login"]) == ([], ["sphinx_password"])


def test_oracle_and_postgresql_should_share_the_same_behaviour():
    assert oracle_schema_delta(["id", "login"], ["id", "login", "mfa_policy"]) == (["mfa_policy"], [])
