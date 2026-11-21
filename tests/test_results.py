import pytest
from pyspark.sql.types import ArrayType, StringType, StructField

from retro_speedlab_lakehouse.results.postgres import column


def test_a_column_postgres_has_no_type_for_fails() -> None:
    with pytest.raises(ValueError, match="tags"):
        column(StructField("tags", ArrayType(StringType())))
