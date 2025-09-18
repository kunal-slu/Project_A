import logging

logger = logging.getLogger(__name__)


class DQChecks:
    def assert_non_null(self, df, col_name):
        null_count = df.filter(df[col_name].isNull()).count()
        if null_count > 0:
            raise ValueError(f"Nulls found in required column: {col_name}")
        logger.info(f"No nulls in required column: {col_name}")

    def assert_unique(self, df, col_name):
        total = df.count()
        unique = df.select(col_name).distinct().count()
        if total != unique:
            raise ValueError(f"Non-unique values found in column: {col_name}")
        logger.info(f"All values unique in column: {col_name}")

    def assert_age_range(self, df, min_age, max_age):
        bad_count = df.filter((df.age < min_age) | (df.age > max_age)).count()
        if bad_count > 0:
            logger.warning(f"{bad_count} records with out-of-range age")
        return bad_count

    def assert_email_valid(self, df, email_regex):
        from pyspark.sql.functions import col

        bad_count = df.filter(
            ~(col("email").isNotNull() & col("email").rlike(email_regex))
        ).count()
        if bad_count > 0:
            logger.warning(f"{bad_count} records with invalid email")
        return bad_count

    def assert_referential_integrity(self, child_df, child_key, parent_df, parent_key):
        """Check that every child_key exists in parent_key. Returns count of violations."""
        from pyspark.sql.functions import col

        parent_alias = parent_df.select(
            col(parent_key).alias("__parent_key")
        ).distinct()
        missing = child_df.join(
            parent_alias,
            child_df[child_key] == col("__parent_key"),
            "left_anti",
        )
        cnt = missing.count()
        if cnt > 0:
            logger.warning(
                f"Referential integrity violations: {cnt} rows in child missing parent {parent_key}"
            )
        else:
            logger.info("Referential integrity OK")
        return cnt

    def assert_values_in_set(self, df, col_name, allowed_values):
        from pyspark.sql.functions import col

        bad = df.filter(~col(col_name).isin(list(allowed_values)))
        cnt = bad.count()
        if cnt > 0:
            logger.warning(f"{cnt} records have unexpected values in {col_name}")
        return cnt
