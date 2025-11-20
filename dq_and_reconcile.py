import os
import sqlite3

import numpy as np
import pandas as pd


def main() -> None:
    """
    Run data quality checks, premium reconciliation, and build reporting datasets.

    Expects:
      - data/policies.csv
      - data/accounting_gl.csv
      - data/claims.csv

    Produces:
      - output/data_quality_issues.csv
      - output/reconciliation_results.csv
      - output/reporting_dataset.csv
    """
    os.makedirs("output", exist_ok=True)

    # Connect to a local SQLite database file
    # con = sqlite3.connect("obie_finance.db")
    con = sqlite3.connect(":memory:")


    # Load base data
    policies = pd.read_csv("data/policies.csv")
    gl = pd.read_csv("data/accounting_gl.csv")
    claims = pd.read_csv("data/claims.csv")

    # Write to SQLite so we can use SQL
    policies.to_sql("policies", con, if_exists="replace", index=False)
    gl.to_sql("accounting_gl", con, if_exists="replace", index=False)
    claims.to_sql("claims", con, if_exists="replace", index=False)

    dq_issues: list[dict] = []

    def add_issues(df: pd.DataFrame, table_name: str, check_name: str) -> None:
        """Add each failing row as an issue."""
        for _, row in df.iterrows():
            dq_issues.append(
                {
                    "table_name": table_name,
                    "check_name": check_name,
                    "policy_id": row.get("policy_id"),
                    "issue_detail": str(dict(row)),
                }
            )

    # 1. Duplicate policy ids in policies
    dup_policies = pd.read_sql_query(
        """
        SELECT policy_id, COUNT(*) AS cnt
        FROM policies
        GROUP BY policy_id
        HAVING COUNT(*) > 1
        """,
        con,
    )
    add_issues(dup_policies, "policies", "duplicate_policy_id")

    # 2. Nulls in key fields in policies
    null_policies = pd.read_sql_query(
        """
        SELECT *
        FROM policies
        WHERE policy_id IS NULL OR written_premium IS NULL
        """,
        con,
    )
    add_issues(null_policies, "policies", "null_key_or_premium")

    # 3. Nulls in key fields in GL
    null_gl = pd.read_sql_query(
        """
        SELECT *
        FROM accounting_gl
        WHERE policy_id IS NULL OR premium_booked IS NULL
        """,
        con,
    )
    add_issues(null_gl, "accounting_gl", "null_key_or_premium")

    # 4. Negative premiums in GL
    neg_gl = pd.read_sql_query(
        """
        SELECT *
        FROM accounting_gl
        WHERE premium_booked < 0
        """,
        con,
    )
    add_issues(neg_gl, "accounting_gl", "negative_premium_booked")

    # 5. Claims data quality checks
    claims_null = pd.read_sql_query(
        """
        SELECT *
        FROM claims
        WHERE claim_id IS NULL OR policy_id IS NULL
        """,
        con,
    )
    add_issues(claims_null, "claims", "null_claim_or_policy_id")

    claims_incurred_neg = pd.read_sql_query(
        """
        SELECT *
        FROM claims
        WHERE incurred_loss < 0
        """,
        con,
    )
    add_issues(claims_incurred_neg, "claims", "negative_incurred_loss")

    claims_paid_gt = pd.read_sql_query(
        """
        SELECT *
        FROM claims
        WHERE paid_loss > incurred_loss
        """,
        con,
    )
    add_issues(claims_paid_gt, "claims", "paid_greater_than_incurred")

    claims_reserve_mismatch = pd.read_sql_query(
        """
        SELECT *
        FROM claims
        WHERE ABS(reserve - (incurred_loss - paid_loss)) > 0.01
        """,
        con,
    )
    add_issues(claims_reserve_mismatch, "claims", "reserve_mismatch")

    # Create DQ issues DataFrame
    dq_df = pd.DataFrame(dq_issues)
    dq_df.to_csv("output/data_quality_issues.csv", index=False)
    print(
        f"Data quality issues saved to output/data_quality_issues.csv. "
        f"Total issues: {len(dq_df)}"
    )

    # 6. Reconciliation between policy and GL
    recon_sql = """
    WITH policy_premium AS (
        SELECT policy_id, SUM(written_premium) AS premium_policy
        FROM policies
        GROUP BY policy_id
    ),
    gl_premium AS (
        SELECT policy_id, SUM(premium_booked) AS premium_gl
        FROM accounting_gl
        GROUP BY policy_id
    ),
    all_ids AS (
        SELECT policy_id FROM policy_premium
        UNION
        SELECT policy_id FROM gl_premium
    )
    SELECT
        a.policy_id,
        p.premium_policy,
        g.premium_gl,
        (g.premium_gl - p.premium_policy) AS diff
    FROM all_ids a
    LEFT JOIN policy_premium p ON a.policy_id = p.policy_id
    LEFT JOIN gl_premium g ON a.policy_id = g.policy_id
    """

    recon_df = pd.read_sql_query(recon_sql, con)

    # Drop rows where policy_id is null
    recon_df = recon_df[recon_df["policy_id"].notna()].copy()

    # Calculate percentage difference
    recon_df["diff_pct"] = recon_df["diff"] / recon_df["premium_policy"].replace({0: np.nan})

    def flag_reason(row: pd.Series) -> str:
        if pd.isna(row["premium_policy"]):
            return "Missing in policies"
        if pd.isna(row["premium_gl"]):
            return "Missing in GL"
        if abs(row["diff"]) > 50:
            return "Large difference"
        return "OK"

    recon_df["flag_reason"] = recon_df.apply(flag_reason, axis=1)

    recon_df.to_csv("output/reconciliation_results.csv", index=False)
    print("Reconciliation results saved to output/reconciliation_results.csv")

    # 7. Reporting dataset by booking_date and state
    report_sql = """
    WITH joined AS (
        SELECT
            p.policy_id,
            p.state,
            DATE(gl.booking_date) AS booking_date,
            p.written_premium,
            gl.premium_booked
        FROM policies p
        JOIN accounting_gl gl
          ON p.policy_id = gl.policy_id
    )
    SELECT
        booking_date,
        state,
        SUM(written_premium) AS total_policy_premium,
        SUM(premium_booked) AS total_gl_premium,
        SUM(premium_booked) - SUM(written_premium) AS variance
    FROM joined
    GROUP BY booking_date, state
    ORDER BY booking_date, state
    """

    report_df = pd.read_sql_query(report_sql, con)
    report_df.to_csv("output/reporting_dataset.csv", index=False)
    print("Reporting dataset saved to output/reporting_dataset.csv")

    con.close()
    print("All processing complete.")


if __name__ == "__main__":
    main()
