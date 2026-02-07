#!/usr/bin/env python3
"""
Regenerate all source data files with clean, consistent synthetic data.

This script creates realistic, aligned data across all sources:
- CRM (accounts, contacts, opportunities)
- Snowflake (customers, orders, products)
- Redshift (customer behavior)
- Kafka (order events)
- FX (exchange rates)
"""

import csv
import json
import random
import uuid
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Dict, Any

# Set seed for reproducibility
random.seed(42)

# Base paths
PROJECT_ROOT = Path(__file__).parent.parent
SAMPLES_DIR = PROJECT_ROOT / "aws" / "data" / "samples"

# Customer ID range (aligned across all sources)
CUSTOMER_ID_START = 1
CUSTOMER_ID_END = 50000
PRODUCT_ID_START = 0
PRODUCT_ID_END = 9999
ORDER_ID_START = 1
ORDER_ID_END = 100000

# Data constants
SEGMENTS = ["ENTERPRISE", "SMB", "MIDMARKET", "PUBLIC", "UNKNOWN"]
INDUSTRIES = ["Finance", "Retail", "Tech", "Manufacturing", "Healthcare", "Education", "Energy", "Media"]
COUNTRIES = ["US", "CA", "GB", "IN", "AU", "DE", "FR", "JP", "CN", "BR"]
CURRENCIES = ["USD", "EUR", "GBP", "JPY", "AUD", "CAD"]
PRODUCT_CATEGORIES = ["Books", "Sports", "Electronics", "Clothing", "Home", "Toys", "Automotive", "Food"]
EVENT_TYPES = ["ORDER_CREATED", "PAYMENT_AUTHORIZED", "PAYMENT_CAPTURED", "SHIPPED", "DELIVERED", "CANCELLED"]
CHANNELS = ["WEB", "MOBILE", "CALL_CENTER", "PARTNER"]
OPPORTUNITY_STAGES = ["PIPELINE", "PROPOSAL", "CLOSED_WON", "CLOSED_LOST"]
CONTACT_ROLES = ["CIO", "CTO", "VP DATA", "ANALYST", "MANAGER", "DIRECTOR", "EXECUTIVE"]


def generate_customer_id(cust_num: int) -> str:
    """Generate normalized customer ID."""
    return f"CUST-{cust_num:06d}"


def generate_product_id(prod_num: int) -> str:
    """Generate normalized product ID."""
    return f"PROD-{prod_num:06d}"


def generate_order_id(order_num: int) -> str:
    """Generate normalized order ID."""
    return f"ORD-{order_num:08d}"


def generate_account_id(account_num: int) -> str:
    """Generate account ID that maps to customer_id."""
    return generate_customer_id(account_num)


def generate_contact_id(contact_num: int) -> str:
    """Generate contact ID."""
    return f"CONT-{contact_num:08d}"


def generate_opportunity_id(opp_num: int) -> str:
    """Generate opportunity ID."""
    return f"OPP-{opp_num:08d}"


def generate_event_id() -> str:
    """Generate Kafka event ID."""
    return f"KAFKA-{uuid.uuid4()}"


def random_date(start_date: datetime, end_date: datetime) -> str:
    """Generate random date string."""
    delta = end_date - start_date
    days = random.randint(0, delta.days)
    date = start_date + timedelta(days=days)
    return date.strftime("%Y-%m-%d")


def random_datetime(start_date: datetime, end_date: datetime) -> str:
    """Generate random datetime string."""
    delta = end_date - start_date
    seconds = random.randint(0, int(delta.total_seconds()))
    dt = start_date + timedelta(seconds=seconds)
    return dt.strftime("%Y-%m-%d %H:%M:%S")


def generate_crm_accounts(num_accounts: int = 30153) -> List[Dict[str, Any]]:
    """Generate CRM accounts data aligned with customer IDs."""
    accounts = []
    start_date = datetime(2010, 1, 1)
    end_date = datetime.now()
    
    for i in range(1, num_accounts + 1):
        customer_id = generate_customer_id(i)
        created_date = random_date(start_date, end_date)
        
        accounts.append({
            "Id": customer_id,  # Use customer_id format for alignment
            "Name": f"Company {i:05d}",
            "Phone": f"({random.randint(100,999)}){random.randint(100,999)}-{random.randint(1000,9999)}",
            "Website": f"https://company{i}.com",
            "Industry": random.choice(INDUSTRIES),
            "AnnualRevenue": random.uniform(100000, 500000000),
            "NumberOfEmployees": random.randint(10, 50000),
            "BillingStreet": f"{random.randint(1,9999)} Main St",
            "BillingCity": f"City{i % 100}",
            "BillingState": random.choice(["CA", "NY", "TX", "FL", "IL"]),
            "BillingPostalCode": f"{random.randint(10000,99999)}",
            "BillingCountry": random.choice(COUNTRIES),
            "Rating": random.choice(["Hot", "Warm", "Cold"]),
            "Type": random.choice(["Customer", "Partner", "Prospect"]),
            "AccountSource": random.choice(["Web", "Referral", "Partner"]),
            "AccountNumber": f"ACC-{i:06d}",
            "Site": f"site{i}.com",
            "Description": f"Account description for {customer_id}",
            "Ownership": random.choice(["Public", "Private", "Other"]),
            "ParentId": "",
            "TickerSymbol": f"TICK{i % 1000:03d}" if random.random() > 0.7 else "",
            "YearStarted": random.randint(1980, 2020),
            "CreatedDate": created_date,
            "LastModifiedDate": random_date(datetime.fromisoformat(created_date), end_date),
            "OwnerId": f"USER{random.randint(1,100):03d}",
            "CustomerSegment": random.choice(SEGMENTS),
            "GeographicRegion": random.choice(["North America", "Europe", "Asia", "Middle East", "South America"]),
            "AccountStatus": random.choice(["Active", "Inactive", "Prospect"]),
            "LastActivityDate": random_date(datetime.fromisoformat(created_date), end_date),
        })
    
    return accounts


def generate_crm_contacts(num_contacts: int = 45000) -> List[Dict[str, Any]]:
    """Generate CRM contacts data."""
    contacts = []
    start_date = datetime(2010, 1, 1)
    end_date = datetime.now()
    
    # Map contacts to accounts (multiple contacts per account)
    account_contact_map = {}
    contact_num = 1
    
    for account_id in range(1, min(30153, CUSTOMER_ID_END) + 1):
        customer_id = generate_customer_id(account_id)
        num_contacts_for_account = random.randint(1, 3)
        
        for _ in range(num_contacts_for_account):
            if contact_num > num_contacts:
                break
            
            created_date = random_date(start_date, end_date)
            first_name = f"Contact{contact_num}"
            last_name = f"Person{contact_num}"
            
            contacts.append({
                "Id": generate_contact_id(contact_num),
                "LastName": last_name,
                "AccountId": customer_id,  # Aligned with customer_id
                "FirstName": first_name,
                "Email": f"{first_name.lower()}.{last_name.lower()}@company{account_id}.com",
                "Phone": f"({random.randint(100,999)}){random.randint(100,999)}-{random.randint(1000,9999)}",
                "MobilePhone": f"({random.randint(100,999)}){random.randint(100,999)}-{random.randint(1000,9999)}",
                "Title": random.choice(CONTACT_ROLES),
                "Department": random.choice(["IT", "Sales", "Marketing", "Operations", "Finance"]),
                "LeadSource": random.choice(["Web", "Referral", "Partner", "Trade Show"]),
                "MailingStreet": f"{random.randint(1,9999)} Contact St",
                "MailingCity": f"City{contact_num % 100}",
                "MailingState": random.choice(["CA", "NY", "TX", "FL", "IL"]),
                "MailingPostalCode": f"{random.randint(10000,99999)}",
                "MailingCountry": random.choice(COUNTRIES),
                "DoNotCall": random.choice([True, False]),
                "HasOptedOutOfEmail": random.choice([True, False]),
                "Description": f"Contact for {customer_id}",
                "CreatedDate": created_date,
                "LastModifiedDate": random_date(datetime.fromisoformat(created_date), end_date),
                "OwnerId": f"USER{random.randint(1,100):03d}",
                "ContactRole": random.choice(["Decision Maker", "Influencer", "User", "Champion"]),
                "ContactLevel": random.choice(["C-Level", "VP", "Director", "Manager", "Individual"]),
                "EngagementScore": random.uniform(0.0, 100.0),
            })
            
            contact_num += 1
            if contact_num > num_contacts:
                break
    
    return contacts


def generate_crm_opportunities(num_opps: int = 20000) -> List[Dict[str, Any]]:
    """Generate CRM opportunities data."""
    opportunities = []
    start_date = datetime(2020, 1, 1)
    end_date = datetime.now()
    
    for i in range(1, num_opps + 1):
        # Map to existing accounts
        account_id = random.randint(1, min(30153, CUSTOMER_ID_END))
        customer_id = generate_customer_id(account_id)
        created_date = random_date(start_date, end_date)
        stage = random.choice(OPPORTUNITY_STAGES)
        is_closed = stage in ["CLOSED_WON", "CLOSED_LOST"]
        is_won = stage == "CLOSED_WON"
        
        opportunities.append({
            "Id": generate_opportunity_id(i),
            "Name": f"Opportunity {i:06d}",
            "AccountId": customer_id,  # Aligned with customer_id
            "StageName": stage,
            "CloseDate": random_date(datetime.fromisoformat(created_date), end_date) if is_closed else "",
            "Amount": random.uniform(10000, 5000000) if is_won else random.uniform(5000, 2000000),
            "Probability": 100 if is_won else (0 if stage == "CLOSED_LOST" else random.randint(10, 90)),
            "LeadSource": random.choice(["Web", "Referral", "Partner", "Trade Show"]),
            "Type": random.choice(["New Business", "Existing Business", "Renewal"]),
            "NextStep": f"Follow up on {customer_id}",
            "Description": f"Opportunity description for {customer_id}",
            "ForecastCategory": random.choice(["Pipeline", "Best Case", "Commit", "Closed"]),
            "IsClosed": is_closed,
            "IsWon": is_won,
            "CreatedDate": created_date,
            "LastModifiedDate": random_date(datetime.fromisoformat(created_date), end_date),
            "OwnerId": f"USER{random.randint(1,100):03d}",
            "DealSize": random.choice(["Small", "Medium", "Large", "Enterprise"]),
            "SalesCycle": random.randint(30, 365),
            "ProductInterest": random.choice(PRODUCT_CATEGORIES),
            "Budget": random.choice(["Approved", "Pending", "Unknown"]),
            "Timeline": random.choice(["Q1", "Q2", "Q3", "Q4", "Immediate"]),
        })
    
    return opportunities


def generate_snowflake_customers(num_customers: int = 50000) -> List[Dict[str, Any]]:
    """Generate Snowflake customers data."""
    customers = []
    start_date = datetime(2010, 1, 1)
    end_date = datetime.now()
    
    for i in range(CUSTOMER_ID_START, num_customers + 1):
        customer_id = generate_customer_id(i)
        created_at = random_datetime(start_date, end_date)
        
        customers.append({
            "customer_id": customer_id,
            "first_name": f"Customer{i % 1000}",
            "last_name": f"Last{i % 500}",
            "email": f"customer{i}@example.com",
            "phone": f"{random.randint(100,999)}.{random.randint(100,999)}.{random.randint(1000,9999)}",
            "address": f"{random.randint(1,9999)} Customer St",
            "city": f"City{i % 200}",
            "state": random.choice(["CA", "NY", "TX", "FL", "IL", "PA", "OH", "GA"]),
            "country": random.choice(COUNTRIES),
            "zip_code": f"{random.randint(10000,99999)}",
            "registration_date": random_date(start_date, end_date),
            "gender": random.choice(["M", "F", "Other"]),
            "age": random.randint(18, 80),
            "customer_segment": random.choice(SEGMENTS),
            "lifetime_value": random.uniform(0, 100000),
            "last_purchase_date": random_date(datetime(2023, 1, 1), end_date) if random.random() > 0.1 else "",
            "total_orders": random.randint(0, 100),
            "preferred_channel": random.choice(["Email", "SMS", "Phone", "Web"]),
            "created_at": created_at,
            "updated_at": random_datetime(datetime.fromisoformat(created_at.split()[0]), end_date),
            "source_system": "snowflake",
            "ingestion_timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f"),
            "estimated_clv": random.uniform(0, 150000),
        })
    
    return customers


def generate_snowflake_products(num_products: int = 10000) -> List[Dict[str, Any]]:
    """Generate Snowflake products data."""
    products = []
    
    for i in range(PRODUCT_ID_START, num_products):
        product_id = generate_product_id(i)
        
        products.append({
            "product_id": product_id,
            "product_name": f"Product {i % 1000} - {random.choice(PRODUCT_CATEGORIES)}",
            "category": random.choice(PRODUCT_CATEGORIES),
            "price_usd": round(random.uniform(10.0, 2000.0), 2),
            "currency": "USD",
            "description": f"Description for {product_id}",
            "brand": f"Brand{i % 50}",
            "sku": f"SKU-{i:08d}",
            "in_stock": random.choice([True, False]),
            "stock_quantity": random.randint(0, 1000),
            "created_at": random_date(datetime(2020, 1, 1), datetime.now()),
            "updated_at": random_date(datetime(2020, 1, 1), datetime.now()),
        })
    
    return products


def generate_snowflake_orders(num_orders: int = 100000) -> List[Dict[str, Any]]:
    """Generate Snowflake orders data."""
    orders = []
    start_date = datetime(2023, 1, 1)  # Last ~24 months
    end_date = datetime.now()
    
    # Ensure we have valid customer and product IDs
    valid_customer_ids = list(range(CUSTOMER_ID_START, min(50000, CUSTOMER_ID_END) + 1))
    valid_product_ids = list(range(PRODUCT_ID_START, PRODUCT_ID_END))
    
    for i in range(ORDER_ID_START, num_orders + 1):
        order_id = generate_order_id(i)
        customer_id = generate_customer_id(random.choice(valid_customer_ids))
        product_id = generate_product_id(random.choice(valid_product_ids))
        currency = random.choice(CURRENCIES)
        order_timestamp = random_datetime(start_date, end_date)
        quantity = random.randint(1, 10)
        amount_orig = round(random.uniform(20.0, 5000.0), 2)
        
        orders.append({
            "order_id": order_id,
            "customer_id": customer_id,
            "product_id": product_id,
            "currency": currency,
            "total_amount": amount_orig,  # Note: using total_amount, not amount
            "quantity": quantity,
            "order_date": order_timestamp.split()[0],  # Extract date
            "order_timestamp": order_timestamp,
            "status": random.choice(["COMPLETED", "PENDING", "CANCELLED", "SHIPPED"]),
            "payment_method": random.choice(["CREDIT_CARD", "DEBIT_CARD", "PAYPAL", "BANK_TRANSFER"]),
            "shipping_address": f"{random.randint(1,9999)} Order St",
            "shipping_city": f"City{i % 200}",
            "shipping_country": random.choice(COUNTRIES),
        })
    
    return orders


def generate_redshift_behavior(num_records: int = 50000) -> List[Dict[str, Any]]:
    """Generate Redshift customer behavior data."""
    behavior = []
    start_date = datetime(2023, 1, 1)
    end_date = datetime.now()
    
    valid_customer_ids = list(range(CUSTOMER_ID_START, min(50000, CUSTOMER_ID_END) + 1))
    event_names = ["login", "page_view", "purchase", "add_to_cart", "search", "view_product"]
    
    for i in range(1, num_records + 1):
        customer_id = generate_customer_id(random.choice(valid_customer_ids))
        event_timestamp = random_datetime(start_date, end_date)
        event_name = random.choice(event_names)
        
        behavior.append({
            "behavior_id": f"BEH-{i:08d}",
            "customer_id": customer_id,
            "event_name": event_name,
            "event_timestamp": event_timestamp,
            "session_id": f"SESS-{random.randint(100000,999999)}",
            "page_url": f"https://example.com/page/{random.randint(1,100)}",
            "referrer": f"https://referrer{i % 10}.com" if random.random() > 0.3 else "",
            "device_type": random.choice(["desktop", "mobile", "tablet"]),
            "browser": random.choice(["Chrome", "Firefox", "Safari", "Edge"]),
            "os": random.choice(["Windows", "macOS", "Linux", "iOS", "Android"]),
            "country": random.choice(COUNTRIES),
            "city": f"City{i % 200}",
            "user_agent": f"Mozilla/5.0 ({random.choice(['Windows', 'Mac', 'Linux'])})",
            "ip_address": f"{random.randint(1,255)}.{random.randint(1,255)}.{random.randint(1,255)}.{random.randint(1,255)}",
            "duration_seconds": random.randint(10, 3600),
            "conversion_value": random.uniform(0, 500) if event_name == "purchase" else 0.0,
            "utm_source": random.choice(["google", "facebook", "direct", "email"]) if random.random() > 0.4 else "",
            "utm_medium": random.choice(["cpc", "organic", "email", "social"]) if random.random() > 0.4 else "",
            "utm_campaign": f"campaign{i % 20}" if random.random() > 0.5 else "",
            "behavior_segment": random.choice(["high_value", "medium_value", "low_value", "new"]),
            "conversion_probability": random.uniform(0.0, 1.0),
            "journey_stage": random.choice(["awareness", "consideration", "purchase", "retention"]),
            "engagement_score": random.uniform(0.0, 100.0),
            "session_quality": random.choice(["high", "medium", "low"]),
        })
    
    return behavior


def generate_kafka_events(num_events: int = 100000) -> List[Dict[str, Any]]:
    """Generate Kafka order events data."""
    events = []
    start_date = datetime(2023, 1, 1)
    end_date = datetime.now()
    
    # Get valid order IDs from generated orders
    valid_order_ids = [generate_order_id(i) for i in range(ORDER_ID_START, min(ORDER_ID_END, 100000) + 1)]
    valid_customer_ids = list(range(CUSTOMER_ID_START, min(50000, CUSTOMER_ID_END) + 1))
    
    # Create order lookup for amounts and currencies
    order_lookup = {}
    for i in range(ORDER_ID_START, min(ORDER_ID_END, 100000) + 1):
        order_id = generate_order_id(i)
        order_lookup[order_id] = {
            "amount": round(random.uniform(20.0, 5000.0), 2),
            "currency": random.choice(CURRENCIES),
        }
    
    for i in range(1, num_events + 1):
        event_id = generate_event_id()
        order_id = random.choice(valid_order_ids)
        customer_id = generate_customer_id(random.choice(valid_customer_ids))
        
        # Get order details
        order_details = order_lookup.get(order_id, {
            "amount": round(random.uniform(20.0, 5000.0), 2),
            "currency": random.choice(CURRENCIES),
        })
        
        event_type = random.choice(EVENT_TYPES)
        event_timestamp = random_datetime(start_date, end_date)
        channel = random.choice(CHANNELS)
        
        # Create JSON value
        value_json = {
            "order_id": order_id,
            "customer_id": customer_id,
            "event_type": event_type,
            "event_ts": event_timestamp,
            "amount": order_details["amount"],
            "currency": order_details["currency"],
            "channel": channel,
            "metadata": {
                "source": "mobile_app" if channel == "MOBILE" else "web_app",
                "version": "2.1.0",
                "session_id": f"SESS-{random.randint(100000,999999)}",
            }
        }
        
        events.append({
            "event_id": event_id,
            "topic": "orders_events",
            "partition": random.randint(0, 5),
            "offset": i,
            "timestamp": event_timestamp,
            "key": f"KEY-{random.randint(100000,999999)}",
            "value": json.dumps(value_json),
            "headers": json.dumps({
                "content-type": "application/json",
                "source": "kafka-producer",
                "version": "1.0"
            }),
        })
    
    return events


def generate_fx_rates_csv(num_days: int = 730) -> List[Dict[str, Any]]:
    """Generate FX rates CSV data."""
    rates = []
    base_date = datetime.now() - timedelta(days=num_days)
    currency_pairs = [
        ("USD", "EUR", 0.85, 1.15),
        ("USD", "GBP", 0.70, 0.90),
        ("USD", "JPY", 90.0, 160.0),
        ("USD", "AUD", 1.20, 1.70),
        ("USD", "CAD", 1.10, 1.40),
    ]
    
    for day in range(num_days):
        trade_date = (base_date + timedelta(days=day)).strftime("%Y-%m-%d")
        
        for base_ccy, quote_ccy, min_rate, max_rate in currency_pairs:
            # Generate realistic rate with slight daily variation
            base_rate = (min_rate + max_rate) / 2
            variation = random.uniform(-0.02, 0.02)  # 2% daily variation
            fx_rate = round(base_rate * (1 + variation), 4)
            fx_rate = max(min_rate, min(max_rate, fx_rate))  # Clamp to range
            
            rates.append({
                "trade_date": trade_date,
                "base_ccy": base_ccy,
                "quote_ccy": quote_ccy,
                "fx_rate": fx_rate,
            })
    
    return rates


def generate_fx_rates_json(num_days: int = 730) -> List[Dict[str, Any]]:
    """Generate FX rates JSON Lines data."""
    rates = []
    base_date = datetime.now() - timedelta(days=num_days)
    currency_pairs = [
        ("USD", "EUR", 0.85, 1.15),
        ("USD", "GBP", 0.70, 0.90),
        ("USD", "JPY", 90.0, 160.0),
        ("USD", "AUD", 1.20, 1.70),
        ("USD", "CAD", 1.10, 1.40),
    ]
    
    for day in range(num_days):
        trade_date = (base_date + timedelta(days=day)).strftime("%Y-%m-%d")
        
        for base_ccy, quote_ccy, min_rate, max_rate in currency_pairs:
            base_rate = (min_rate + max_rate) / 2
            variation = random.uniform(-0.02, 0.02)
            fx_rate = round(base_rate * (1 + variation), 4)
            fx_rate = max(min_rate, min(max_rate, fx_rate))
            
            rates.append({
                "date": trade_date,
                "base_currency": base_ccy,
                "target_currency": quote_ccy,
                "exchange_rate": fx_rate,
                "bid_rate": round(fx_rate * 0.9995, 4),
                "ask_rate": round(fx_rate * 1.0005, 4),
                "source": "synthetic",
            })
    
    return rates


def write_csv(filepath: Path, data: List[Dict[str, Any]], fieldnames: List[str]):
    """Write data to CSV file."""
    filepath.parent.mkdir(parents=True, exist_ok=True)
    
    with open(filepath, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(data)
    
    print(f"✅ Wrote {len(data):,} rows to {filepath}")


def write_json_lines(filepath: Path, data: List[Dict[str, Any]]):
    """Write data to JSON Lines file."""
    filepath.parent.mkdir(parents=True, exist_ok=True)
    
    with open(filepath, 'w', encoding='utf-8') as f:
        for record in data:
            f.write(json.dumps(record) + '\n')
    
    print(f"✅ Wrote {len(data):,} lines to {filepath}")


def main():
    """Main function to regenerate all source data."""
    print("🔄 Regenerating all source data files...")
    print("=" * 80)
    
    # CRM data
    print("\n📊 Generating CRM data...")
    accounts = generate_crm_accounts(30153)
    write_csv(
        SAMPLES_DIR / "crm" / "accounts.csv",
        accounts,
        list(accounts[0].keys())
    )
    
    contacts = generate_crm_contacts(45000)
    write_csv(
        SAMPLES_DIR / "crm" / "contacts.csv",
        contacts,
        list(contacts[0].keys())
    )
    
    opportunities = generate_crm_opportunities(20000)
    write_csv(
        SAMPLES_DIR / "crm" / "opportunities.csv",
        opportunities,
        list(opportunities[0].keys())
    )
    
    # Snowflake data
    print("\n📊 Generating Snowflake data...")
    customers = generate_snowflake_customers(50000)
    write_csv(
        SAMPLES_DIR / "snowflake" / "snowflake_customers_50000.csv",
        customers,
        list(customers[0].keys())
    )
    
    products = generate_snowflake_products(10000)
    write_csv(
        SAMPLES_DIR / "snowflake" / "snowflake_products_10000.csv",
        products,
        list(products[0].keys())
    )
    
    orders = generate_snowflake_orders(100000)
    write_csv(
        SAMPLES_DIR / "snowflake" / "snowflake_orders_100000.csv",
        orders,
        list(orders[0].keys())
    )
    
    # Redshift data
    print("\n📊 Generating Redshift behavior data...")
    behavior = generate_redshift_behavior(50000)
    write_csv(
        SAMPLES_DIR / "redshift" / "redshift_customer_behavior_50000.csv",
        behavior,
        list(behavior[0].keys())
    )
    
    # Kafka data
    print("\n📊 Generating Kafka events data...")
    kafka_events = generate_kafka_events(100000)
    write_csv(
        SAMPLES_DIR / "kafka" / "stream_kafka_events_100000.csv",
        kafka_events,
        list(kafka_events[0].keys())
    )
    
    # FX rates
    print("\n📊 Generating FX rates data...")
    fx_rates_csv = generate_fx_rates_csv(730)
    write_csv(
        SAMPLES_DIR / "fx" / "fx_rates_historical_730_days.csv",
        fx_rates_csv,
        ["trade_date", "base_ccy", "quote_ccy", "fx_rate"]
    )
    
    fx_rates_json = generate_fx_rates_json(730)
    write_json_lines(
        SAMPLES_DIR / "fx" / "fx_rates_historical.json",
        fx_rates_json
    )
    
    print("\n" + "=" * 80)
    print("✅ All source data files regenerated successfully!")
    print("\nNext steps:")
    print("1. Run: python jobs/transform/bronze_to_silver.py --env local --config local/config/local.yaml")
    print("2. Run: python jobs/transform/silver_to_gold.py --env local --config local/config/local.yaml")
    print("3. Run: python tools/validate_local_etl.py --env local --config local/config/local.yaml")


if __name__ == "__main__":
    main()

