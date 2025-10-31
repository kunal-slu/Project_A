#!/usr/bin/env python3
"""
Generate synthetic CRM (Salesforce) data for local testing.

Uses Faker with seeded random for deterministic, realistic data.
"""

import pandas as pd
import numpy as np
from faker import Faker
from datetime import datetime, timedelta
import argparse
import os

# Seed for reproducibility
Faker.seed(42)
np.random.seed(42)

fake = Faker()


def generate_accounts(count: int = 10000) -> pd.DataFrame:
    """Generate synthetic Account records."""
    accounts = []
    
    industries = ["Technology", "Finance", "Healthcare", "Retail", "Manufacturing", 
                  "Education", "Real Estate", "Consulting", "Energy", "Media"]
    types_list = ["Customer", "Prospect", "Partner", "Competitor"]
    ratings = ["Hot", "Warm", "Cold"]
    
    for i in range(count):
        created_date = fake.date_time_between(start_date="-2y", end_date="now")
        modified_date = fake.date_time_between(start_date=created_date, end_date="now")
        
        accounts.append({
            "Id": f"001{i:08d}",
            "Name": fake.company(),
            "Type": np.random.choice(types_list),
            "ParentId": None if np.random.random() > 0.1 else f"001{np.random.randint(0, i):08d}",
            "BillingStreet": fake.street_address(),
            "BillingCity": fake.city(),
            "BillingState": fake.state_abbr(),
            "BillingPostalCode": fake.zipcode(),
            "BillingCountry": "USA",
            "ShippingStreet": fake.street_address(),
            "ShippingCity": fake.city(),
            "ShippingState": fake.state_abbr(),
            "ShippingPostalCode": fake.zipcode(),
            "ShippingCountry": "USA",
            "Phone": fake.phone_number(),
            "Fax": fake.phone_number() if np.random.random() > 0.7 else None,
            "AccountNumber": f"ACC-{i:06d}",
            "Website": fake.url(),
            "Industry": np.random.choice(industries),
            "AnnualRevenue": np.random.choice([None, np.random.uniform(100000, 100000000)]),
            "NumberOfEmployees": np.random.choice([None, np.random.randint(10, 10000)]),
            "Ownership": np.random.choice(["Public", "Private", "Subsidiary", None]),
            "TickerSymbol": fake.stock_symbol() if np.random.random() > 0.9 else None,
            "Description": fake.text(max_nb_chars=200) if np.random.random() > 0.5 else None,
            "Rating": np.random.choice(ratings),
            "Site": fake.company_suffix(),
            "CreatedDate": created_date.isoformat(),
            "LastModifiedDate": modified_date.isoformat(),
            "IsDeleted": False
        })
    
    return pd.DataFrame(accounts)


def generate_contacts(count: int = 50000, accounts_df: pd.DataFrame = None) -> pd.DataFrame:
    """Generate synthetic Contact records linked to accounts."""
    contacts = []
    
    if accounts_df is None or accounts_df.empty:
        account_ids = [f"001{i:08d}" for i in range(1000)]
    else:
        account_ids = accounts_df["Id"].tolist()
    
    departments = ["Sales", "Marketing", "Engineering", "Support", "Operations", 
                   "Finance", "HR", "Legal", None]
    lead_sources = ["Web", "Referral", "Partner", "Cold Call", "Email", "Trade Show"]
    
    for i in range(count):
        created_date = fake.date_time_between(start_date="-2y", end_date="now")
        modified_date = fake.date_time_between(start_date=created_date, end_date="now")
        
        # Link to account (90% have account, 10% don't)
        account_id = np.random.choice(account_ids) if np.random.random() > 0.1 else None
        
        contacts.append({
            "Id": f"003{i:08d}",
            "FirstName": fake.first_name(),
            "LastName": fake.last_name(),  # REQUIRED by Salesforce
            "Email": fake.email(),
            "Phone": fake.phone_number(),
            "Title": fake.job(),
            "Department": np.random.choice(departments),
            "AccountId": account_id,
            "LeadSource": np.random.choice(lead_sources),
            "MailingStreet": fake.street_address(),
            "MailingCity": fake.city(),
            "MailingState": fake.state_abbr(),
            "MailingPostalCode": fake.zipcode(),
            "MailingCountry": "USA",
            "OtherStreet": fake.street_address() if np.random.random() > 0.7 else None,
            "OtherCity": fake.city() if np.random.random() > 0.7 else None,
            "OtherState": fake.state_abbr() if np.random.random() > 0.7 else None,
            "OtherPostalCode": fake.zipcode() if np.random.random() > 0.7 else None,
            "OtherCountry": "USA" if np.random.random() > 0.7 else None,
            "CreatedDate": created_date.isoformat(),
            "LastModifiedDate": modified_date.isoformat(),
            "IsDeleted": False
        })
    
    return pd.DataFrame(contacts)


def generate_opportunities(count: int = 25000, accounts_df: pd.DataFrame = None) -> pd.DataFrame:
    """Generate synthetic Opportunity records linked to accounts."""
    opportunities = []
    
    if accounts_df is None or accounts_df.empty:
        account_ids = [f"001{i:08d}" for i in range(1000)]
    else:
        account_ids = accounts_df["Id"].tolist()
    
    stages = ["Qualification", "Needs Analysis", "Value Proposition", "Id. Decision Makers",
              "Perception Analysis", "Proposal/Price Quote", "Negotiation/Review",
              "Closed Won", "Closed Lost"]
    types_list = ["New Business", "Existing Business", "Renewal", None]
    lead_sources = ["Web", "Referral", "Partner", "Cold Call"]
    
    for i in range(count):
        created_date = fake.date_time_between(start_date="-2y", end_date="now")
        modified_date = fake.date_time_between(start_date=created_date, end_date="now")
        
        # Close date must be in future for open opportunities
        if np.random.random() > 0.3:  # 70% open opportunities
            close_date = fake.date_between(start_date="today", end_date="+6m")
        else:  # 30% closed opportunities
            close_date = fake.date_between(start_date="-1y", end_date="today")
        
        stage = np.random.choice(stages)
        account_id = np.random.choice(account_ids)
        
        # Amount depends on stage
        if stage in ["Closed Won", "Negotiation/Review"]:
            amount = np.random.uniform(10000, 500000)
            probability = np.random.uniform(75, 100)
        elif stage == "Closed Lost":
            amount = np.random.uniform(5000, 100000)
            probability = 0
        else:
            amount = np.random.uniform(5000, 200000)
            probability = np.random.uniform(10, 75)
        
        opportunities.append({
            "Id": f"006{i:08d}",
            "Name": f"Opportunity {i+1}",
            "AccountId": account_id,
            "Amount": round(amount, 2),
            "StageName": stage,  # REQUIRED
            "Probability": round(probability, 1),
            "CloseDate": close_date.isoformat(),  # REQUIRED
            "Type": np.random.choice(types_list),
            "LeadSource": np.random.choice(lead_sources),
            "OwnerId": f"005{fake.random_int(min=0, max=9999):08d}",
            "Description": fake.text(max_nb_chars=200) if np.random.random() > 0.5 else None,
            "CreatedDate": created_date.isoformat(),
            "LastModifiedDate": modified_date.isoformat(),
            "IsDeleted": False
        })
    
    return pd.DataFrame(opportunities)


def main():
    parser = argparse.ArgumentParser(description="Generate synthetic CRM data")
    parser.add_argument("--accounts", type=int, default=10000, help="Number of accounts")
    parser.add_argument("--contacts", type=int, default=50000, help="Number of contacts")
    parser.add_argument("--opportunities", type=int, default=25000, help="Number of opportunities")
    parser.add_argument("--output-dir", default="data/samples/crm", help="Output directory")
    
    args = parser.parse_args()
    
    # Create output directory
    os.makedirs(args.output_dir, exist_ok=True)
    
    print("Generating synthetic CRM data...")
    print(f"  Accounts: {args.accounts}")
    print(f"  Contacts: {args.contacts}")
    print(f"  Opportunities: {args.opportunities}")
    
    # Generate accounts first (needed for relationships)
    print("\nGenerating accounts...")
    accounts_df = generate_accounts(args.accounts)
    accounts_df.to_csv(f"{args.output_dir}/accounts.csv", index=False)
    print(f"  ✓ Generated {len(accounts_df)} accounts")
    
    # Generate contacts (linked to accounts)
    print("\nGenerating contacts...")
    contacts_df = generate_contacts(args.contacts, accounts_df)
    contacts_df.to_csv(f"{args.output_dir}/contacts.csv", index=False)
    print(f"  ✓ Generated {len(contacts_df)} contacts")
    
    # Generate opportunities (linked to accounts)
    print("\nGenerating opportunities...")
    opportunities_df = generate_opportunities(args.opportunities, accounts_df)
    opportunities_df.to_csv(f"{args.output_dir}/opportunities.csv", index=False)
    print(f"  ✓ Generated {len(opportunities_df)} opportunities")
    
    print(f"\n✅ All CRM data generated in {args.output_dir}/")
    print(f"\nFile sizes:")
    for file in ["accounts.csv", "contacts.csv", "opportunities.csv"]:
        path = f"{args.output_dir}/{file}"
        if os.path.exists(path):
            size_mb = os.path.getsize(path) / (1024 * 1024)
            print(f"  {file}: {size_mb:.2f} MB")


if __name__ == "__main__":
    main()

