import os
import random
from datetime import datetime, timedelta

import numpy as np
import pandas as pd


def main() -> None:
    
    # Ensure data folder exists
    os.makedirs("data", exist_ok=True)

    # Basic lookup values
    num_policies = 50000
    policy_ids = [f"P{1000 + i}" for i in range(num_policies)]
    states = ["IL", "TX", "FL", "GA", "NC"]
    products = ["Landlord", "Short Term Rental", "Multi-Family"]
    brokers = ["Broker A", "Broker B", "Broker C"]
    start_date = datetime(2024, 1, 1)

    
    # 1. POLICIES
    
    policies_rows = []
    for pid in policy_ids:
        eff_date = start_date + timedelta(days=random.randint(0, 180))
        written_premium = round(np.random.uniform(500, 5000), 2)
        row = {
            "policy_id": pid,
            "effective_date": eff_date.date().isoformat(),
            "written_premium": written_premium,
            "product": random.choice(products),
            "state": random.choice(states),
            "broker": random.choice(brokers),
        }
        policies_rows.append(row)

    policies = pd.DataFrame(policies_rows)


    # 2. Accounting GL
   
    gl_rows = []
    for _, row in policies.iterrows():
        pid = row["policy_id"]
        eff_date = datetime.fromisoformat(row["effective_date"])
        booking_date = eff_date + timedelta(days=random.randint(0, 30))

        # Most are equal, some slightly off (to create recon differences)
        factor = random.choice([1.0, 1.0, 1.0, 0.95, 1.05])
        premium_booked = round(row["written_premium"] * factor, 2)
        taxes = round(premium_booked * 0.05, 2)
        fees = round(np.random.uniform(10, 100), 2)

        gl_rows.append(
            {
                "policy_id": pid,
                "booking_date": booking_date.date().isoformat(),
                "premium_booked": premium_booked,
                "taxes": taxes,
                "fees": fees,
            }
        )

    # Add some GL-only policies (no match in policies system)
    extra_ids = [f"X{2000 + i}" for i in range(50)]
    for pid in extra_ids:
        booking_date = start_date + timedelta(days=random.randint(0, 180))
        premium_booked = round(np.random.uniform(500, 5000), 2)
        taxes = round(premium_booked * 0.05, 2)
        fees = round(np.random.uniform(10, 100), 2)

        gl_rows.append(
            {
                "policy_id": pid,
                "booking_date": booking_date.date().isoformat(),
                "premium_booked": premium_booked,
                "taxes": taxes,
                "fees": fees,
            }
        )

    accounting_gl = pd.DataFrame(gl_rows)

    
    # 3. CLAIMS
   
    claims_rows = []

    # Map policy_id -> state so each claim knows its state
    policy_state_map = dict(zip(policies["policy_id"], policies["state"]))

    # Create claims for a subset of policies
    for pid in random.sample(policy_ids, 5000):
        num_claims = random.randint(0, 3)
        for _ in range(num_claims):
            loss_date = start_date + timedelta(days=random.randint(0, 200))
            incurred_loss = round(np.random.uniform(0, 10000), 2)
            paid_loss = round(incurred_loss * random.uniform(0, 1), 2)
            reserve = incurred_loss - paid_loss

            claims_rows.append(
                {
                    "claim_id": f"C{random.randint(10000, 99999)}",
                    "policy_id": pid,
                    "state": policy_state_map.get(pid),
                    "loss_date": loss_date.date().isoformat(),
                    "incurred_loss": incurred_loss,
                    "paid_loss": paid_loss,
                    "reserve": reserve,
                }
            )

    claims = pd.DataFrame(claims_rows)

   
    # 4. Data quality issues
    
    # Policies: random 3–10% bad rows
    bad_frac_policies = random.uniform(0.03, 0.10) if len(policies) > 0 else 0.0
    n_bad_pol = int(bad_frac_policies * len(policies))
    if n_bad_pol > 0:
        bad_idx_pol = np.random.choice(policies.index, size=n_bad_pol, replace=False)
        split1 = int(0.5 * n_bad_pol)

        # first half: missing written_premium
        policies.loc[bad_idx_pol[:split1], "written_premium"] = np.nan
        # second half: missing policy_id
        policies.loc[bad_idx_pol[split1:], "policy_id"] = None

    # GL: random 3–10% bad rows
    bad_frac_gl = random.uniform(0.03, 0.10) if len(accounting_gl) > 0 else 0.0
    n_bad_gl = int(bad_frac_gl * len(accounting_gl))
    if n_bad_gl > 0:
        bad_idx_gl = np.random.choice(accounting_gl.index, size=n_bad_gl, replace=False)
        third = n_bad_gl // 3 if n_bad_gl >= 3 else 1

        # first third: missing policy_id
        accounting_gl.loc[bad_idx_gl[:third], "policy_id"] = None
        # second third: missing premium_booked
        accounting_gl.loc[bad_idx_gl[third:2 * third], "premium_booked"] = np.nan
        # last third: negative premium_booked
        neg_idx = bad_idx_gl[2 * third:]
        accounting_gl.loc[neg_idx, "premium_booked"] = -accounting_gl.loc[neg_idx, "premium_booked"].abs()

    # Claims: random 3–10% bad rows
    bad_frac_claims = random.uniform(0.03, 0.10) if len(claims) > 0 else 0.0
    n_bad_cl = int(bad_frac_claims * len(claims))
    if n_bad_cl > 0:
        bad_idx_cl = np.random.choice(claims.index, size=n_bad_cl, replace=False)
        third_cl = n_bad_cl // 3 if n_bad_cl >= 3 else 1

        # missing claim_id
        claims.loc[bad_idx_cl[:third_cl], "claim_id"] = None

        # paid_loss > incurred_loss
        overpay_idx = bad_idx_cl[third_cl:2 * third_cl]
        claims.loc[overpay_idx, "paid_loss"] = claims.loc[overpay_idx, "incurred_loss"] * 1.5

        # wrong reserve
        claims.loc[bad_idx_cl[2 * third_cl:], "reserve"] = -1.0

    
    # 5. SAVE CSVs
    
    policies.to_csv("data/policies.csv", index=False)
    accounting_gl.to_csv("data/accounting_gl.csv", index=False)
    claims.to_csv("data/claims.csv", index=False)

    print("Synthetic data created in the 'data' folder.")
    print(
        f"Injected bad-row fractions – "
        f"policies: {bad_frac_policies:.3f}, "
        f"gl: {bad_frac_gl:.3f}, "
        f"claims: {bad_frac_claims:.3f}"
    )


if __name__ == "__main__":
    main()
