import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../src")))

from project_a.dq_checks import DQChecks


def test_assert_non_null(spark):
    df = spark.createDataFrame([(1, "A"), (2, None)], ["id", "val"])
    dq = DQChecks()
    try:
        dq.assert_non_null(df, "val")
        assert False, "Should have raised ValueError"
    except ValueError:
        pass


def test_assert_unique(spark):
    df = spark.createDataFrame([(1, "A"), (1, "A")], ["id", "val"])
    dq = DQChecks()
    try:
        dq.assert_unique(df, "id")
        assert False, "Should have raised ValueError"
    except ValueError:
        pass
