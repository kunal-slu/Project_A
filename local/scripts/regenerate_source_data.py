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
PROJECT_ROOT = Path(__file__).resolve().parents[2]
SAMPLES_DIR = PROJECT_ROOT / "aws" / "data" / "samples"
BRONZE_DIR = PROJECT_ROOT / "data" / "bronze"

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

# Realism parameters
NOW = datetime.utcnow()
REALISTIC_START = NOW - timedelta(days=365 * 5)  # last 5 years
RECENT_START = NOW - timedelta(days=365 * 2)  # last 2 years
STREAM_START = NOW - timedelta(days=120)  # last ~4 months

NULL_RATE_LOW = 0.02
NULL_RATE_MED = 0.05
BAD_EMAIL_RATE = 0.02
BAD_PHONE_RATE = 0.03
OUTLIER_RATE = 0.01


def maybe_null(value: Any, rate: float) -> Any:
    """Return empty string with probability `rate`."""
    if random.random() < rate:
        return ""
    return value


def maybe_bad_email(email: str, rate: float) -> str:
    if random.random() < rate:
        return email.replace("@", "_at_")
    return email


def maybe_bad_phone(phone: str, rate: float) -> str:
    if random.random() < rate:
        return phone.replace("-", "").replace(".", "")
    return phone


def pick_weighted(items: list[Any], weight: float = 0.7) -> Any:
    """Pick from first 5% of items with higher probability for skewed distributions."""
    if not items:
        return None
    cutoff = max(1, int(len(items) * 0.05))
    heavy = items[:cutoff]
    if random.random() < weight:
        return random.choice(heavy)
    return random.choice(items[cutoff:])


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
    start_date = REALISTIC_START
    end_date = NOW
    
    for i in range(1, num_accounts + 1):
        customer_id = generate_customer_id(i)
        created_date = random_date(start_date, end_date)
        
        accounts.append({
            "Id": customer_id,  # Use customer_id format for alignment
            "Name": f"Company {i:05d}",
            "Phone": maybe_bad_phone(
                f"({random.randint(100,999)}){random.randint(100,999)}-{random.randint(1000,9999)}",
                BAD_PHONE_RATE,
            ),
            "Website": maybe_null(f"https://company{i}.com", NULL_RATE_LOW),
            "Industry": random.choice(INDUSTRIES),
            "AnnualRevenue": random.uniform(100000, 500000000),
            "NumberOfEmployees": random.randint(10, 50000),
            "BillingStreet": maybe_null(f"{random.randint(1,9999)} Main St", NULL_RATE_MED),
            "BillingCity": maybe_null(f"City{i % 100}", NULL_RATE_LOW),
            "BillingState": random.choice(["CA", "NY", "TX", "FL", "IL"]),
            "BillingPostalCode": f"{random.randint(10000,99999)}",
            "BillingCountry": random.choice(COUNTRIES),
            "Rating": random.choice(["Hot", "Warm", "Cold"]),
            "Type": random.choice(["Customer", "Partner", "Prospect"]),
            "AccountSource": random.choice(["Web", "Referral", "Partner"]),
            "AccountNumber": f"ACC-{i:06d}",
            "Site": f"site{i}.com",
            "Description": maybe_null(f"Account description for {customer_id}", NULL_RATE_LOW),
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
    start_date = REALISTIC_START
    end_date = NOW
    
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
            
            email = f"{first_name.lower()}.{last_name.lower()}@company{account_id}.com"
            contacts.append({
                "Id": generate_contact_id(contact_num),
                "LastName": last_name,
                "AccountId": customer_id,  # Aligned with customer_id
                "FirstName": first_name,
                "Email": maybe_bad_email(email, BAD_EMAIL_RATE),
                "Phone": maybe_bad_phone(
                    f"({random.randint(100,999)}){random.randint(100,999)}-{random.randint(1000,9999)}",
                    BAD_PHONE_RATE,
                ),
                "MobilePhone": maybe_bad_phone(
                    f"({random.randint(100,999)}){random.randint(100,999)}-{random.randint(1000,9999)}",
                    BAD_PHONE_RATE,
                ),
                "Title": random.choice(CONTACT_ROLES),
                "Department": random.choice(["IT", "Sales", "Marketing", "Operations", "Finance"]),
                "LeadSource": random.choice(["Web", "Referral", "Partner", "Trade Show"]),
                "MailingStreet": maybe_null(f"{random.randint(1,9999)} Contact St", NULL_RATE_MED),
                "MailingCity": maybe_null(f"City{contact_num % 100}", NULL_RATE_LOW),
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
    start_date = REALISTIC_START
    end_date = NOW
    
    for i in range(1, num_opps + 1):
        # Map to existing accounts
        account_id = random.randint(1, min(30153, CUSTOMER_ID_END))
        customer_id = generate_customer_id(account_id)
        created_date = random_date(start_date, end_date)
        stage = random.choice(OPPORTUNITY_STAGES)
        is_closed = stage in ["CLOSED_WON", "CLOSED_LOST"]
        is_won = stage == "CLOSED_WON"
        
        base_amount = random.uniform(10000, 5000000) if is_won else random.uniform(5000, 2000000)
        if random.random() < OUTLIER_RATE:
            base_amount *= random.uniform(3, 15)

        opportunities.append({
            "Id": generate_opportunity_id(i),
            "Name": f"Opportunity {i:06d}",
            "AccountId": customer_id,  # Aligned with customer_id
            "StageName": stage,
            "CloseDate": random_date(datetime.fromisoformat(created_date), end_date) if is_closed else "",
            "Amount": round(base_amount, 2),
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
    start_date = REALISTIC_START
    end_date = NOW
    
    for i in range(CUSTOMER_ID_START, num_customers + 1):
        customer_id = generate_customer_id(i)
        created_at = random_datetime(start_date, end_date)
        email = f"customer{i}@example.com"
        
        customers.append({
            "customer_id": customer_id,
            "first_name": f"Customer{i % 1000}",
            "last_name": f"Last{i % 500}",
            "email": maybe_bad_email(email, BAD_EMAIL_RATE),
            "phone": maybe_bad_phone(
                f"{random.randint(100,999)}.{random.randint(100,999)}.{random.randint(1000,9999)}",
                BAD_PHONE_RATE,
            ),
            "address": maybe_null(f"{random.randint(1,9999)} Customer St", NULL_RATE_MED),
            "city": maybe_null(f"City{i % 200}", NULL_RATE_LOW),
            "state": random.choice(["CA", "NY", "TX", "FL", "IL", "PA", "OH", "GA"]),
            "country": random.choice(COUNTRIES),
            "zip_code": f"{random.randint(10000,99999)}",
            "registration_date": random_date(start_date, end_date),
            "gender": random.choice(["M", "F", "Other"]),
            "age": random.randint(18, 80),
            "customer_segment": random.choice(SEGMENTS),
            "lifetime_value": random.uniform(0, 100000),
            "last_purchase_date": random_date(RECENT_START, end_date) if random.random() > 0.1 else "",
            "total_orders": random.randint(0, 100),
            "preferred_channel": random.choice(["Email", "SMS", "Phone", "Web"]),
            "created_at": created_at,
            "updated_at": random_datetime(datetime.fromisoformat(created_at.split()[0]), end_date),
            "source_system": "snowflake",
            "ingestion_timestamp": NOW.strftime("%Y-%m-%d %H:%M:%S.%f"),
            "estimated_clv": random.uniform(0, 150000),
        })
    
    return customers


def generate_snowflake_products(num_products: int = 10000) -> List[Dict[str, Any]]:
    """Generate Snowflake products data."""
    products = []
    
    for i in range(PRODUCT_ID_START, num_products):
        product_id = generate_product_id(i)
        category = random.choice(PRODUCT_CATEGORIES)
        subcategory = f"{category}_sub_{i % 10}"
        price = round(random.uniform(10.0, 2000.0), 2)
        cost = round(price * random.uniform(0.4, 0.8), 2)
        created_at = random_date(REALISTIC_START, NOW)
        updated_at = random_date(datetime.fromisoformat(created_at), NOW)
        
        products.append({
            "product_id": product_id,
            "sku": f"SKU-{i:08d}",
            "product_name": f"Product {i % 1000} - {category}",
            "category": category,
            "subcategory": subcategory,
            "brand": f"Brand{i % 50}",
            "price_usd": price,
            "cost_usd": cost,
            "weight_kg": round(random.uniform(0.1, 25.0), 2),
            "dimensions": f"{random.randint(5,80)}x{random.randint(5,80)}x{random.randint(1,40)}",
            "color": random.choice(["red", "blue", "green", "black", "white", "yellow"]),
            "size": random.choice(["XS", "S", "M", "L", "XL"]),
            "active": random.choice([True, True, True, False]),
            "stock_quantity": random.randint(0, 1000),
            "reorder_level": random.randint(10, 200),
            "supplier_id": f"SUP-{random.randint(1,2000):05d}",
            "created_at": created_at,
            "updated_at": updated_at,
            "source_system": "snowflake",
            "ingestion_timestamp": NOW.strftime("%Y-%m-%d %H:%M:%S.%f"),
            "rating": round(random.uniform(1.0, 5.0), 1),
            "review_count": random.randint(0, 5000),
            "in_stock": random.choice([True, True, True, False]),
        })
    
    return products


def generate_snowflake_orders(num_orders: int = 100000) -> List[Dict[str, Any]]:
    """Generate Snowflake orders data."""
    orders = []
    start_date = RECENT_START
    end_date = NOW
    
    # Ensure we have valid customer and product IDs
    valid_customer_ids = [
        generate_customer_id(i) for i in range(CUSTOMER_ID_START, min(50000, CUSTOMER_ID_END) + 1)
    ]
    valid_product_ids = [
        generate_product_id(i) for i in range(PRODUCT_ID_START, PRODUCT_ID_END)
    ]

    product_price_lookup = {
        pid: round(random.uniform(10.0, 2000.0), 2) for pid in valid_product_ids
    }

    order_counts: Dict[str, int] = {}
    
    for i in range(ORDER_ID_START, num_orders + 1):
        order_id = generate_order_id(i)
        customer_id = pick_weighted(valid_customer_ids, weight=0.7)
        product_id = pick_weighted(valid_product_ids, weight=0.6)
        currency = random.choice(CURRENCIES)
        order_timestamp = random_datetime(start_date, end_date)
        quantity = random.randint(1, 10)
        unit_price = product_price_lookup.get(product_id, round(random.uniform(10.0, 2000.0), 2))
        subtotal = unit_price * quantity
        discount_percent = random.choice([0, 0, 0, 5, 10, 15, 20])
        discount_amount = subtotal * (discount_percent / 100.0)
        tax_amount = round((subtotal - discount_amount) * 0.08, 2)
        shipping_cost = round(random.uniform(5.0, 30.0), 2)
        total_amount = round(subtotal - discount_amount + tax_amount + shipping_cost, 2)
        if random.random() < OUTLIER_RATE:
            total_amount = round(total_amount * random.uniform(3, 8), 2)

        order_date = order_timestamp.split()[0]
        order_year = int(order_date.split("-")[0])
        order_month = int(order_date.split("-")[1])
        order_quarter = (order_month - 1) // 3 + 1
        created_at = order_timestamp
        updated_at = random_datetime(datetime.fromisoformat(order_date), end_date)
        order_counts[customer_id] = order_counts.get(customer_id, 0) + 1
        order_segment = "high_value" if total_amount >= 1000 else "mid_value" if total_amount >= 200 else "low_value"
        fulfillment_days = random.randint(1, 14)
        status = random.choices(
            ["COMPLETED", "SHIPPED", "PENDING", "CANCELLED"],
            weights=[0.6, 0.2, 0.15, 0.05],
        )[0]

        orders.append({
            "order_id": order_id,
            "customer_id": customer_id,
            "product_id": product_id,
            "order_date": order_date,
            "order_timestamp": order_timestamp,
            "quantity": quantity,
            "unit_price": unit_price,
            "total_amount": total_amount,
            "currency": currency,
            "payment_method": random.choice(["CREDIT_CARD", "DEBIT_CARD", "PAYPAL", "BANK_TRANSFER"]),
            "shipping_method": random.choice(["GROUND", "2_DAY", "OVERNIGHT"]),
            "status": status,
            "shipping_address": maybe_null(f"{random.randint(1,9999)} Order St", NULL_RATE_MED),
            "billing_address": maybe_null(f"{random.randint(1,9999)} Billing St", NULL_RATE_MED),
            "discount_percent": discount_percent,
            "tax_amount": tax_amount,
            "shipping_cost": shipping_cost,
            "promo_code": maybe_null(f"PROMO{random.randint(1,500):03d}", NULL_RATE_MED),
            "sales_rep_id": f"REP-{random.randint(1,200):04d}",
            "channel": random.choice(CHANNELS),
            "created_at": created_at,
            "updated_at": updated_at,
            "source_system": "snowflake",
            "ingestion_timestamp": NOW.strftime("%Y-%m-%d %H:%M:%S.%f"),
            "order_month": order_month,
            "order_quarter": order_quarter,
            "order_year": order_year,
            "order_segment": order_segment,
            "customer_order_count": order_counts[customer_id],
            "fulfillment_days": fulfillment_days if status in ["SHIPPED", "COMPLETED"] else None,
        })
    
    return orders


def generate_redshift_behavior(num_records: int = 50000) -> List[Dict[str, Any]]:
    """Generate Redshift customer behavior data."""
    behavior = []
    start_date = RECENT_START
    end_date = NOW
    
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
            "city": maybe_null(f"City{i % 200}", NULL_RATE_LOW),
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


def generate_kafka_events(
    num_events: int = 100000,
    orders: List[Dict[str, Any]] | None = None,
) -> List[Dict[str, Any]]:
    """Generate Kafka order events data."""
    events = []
    start_date = STREAM_START
    end_date = NOW
    
    # Create order lookup for amounts and currencies
    order_lookup = {}
    if orders:
        for row in orders:
            order_lookup[row["order_id"]] = {
                "amount": row.get("total_amount", round(random.uniform(20.0, 5000.0), 2)),
                "currency": row.get("currency", random.choice(CURRENCIES)),
                "customer_id": row.get("customer_id"),
            }

    if not order_lookup:
        for i in range(ORDER_ID_START, min(ORDER_ID_END, 100000) + 1):
            order_id = generate_order_id(i)
            order_lookup[order_id] = {
                "amount": round(random.uniform(20.0, 5000.0), 2),
                "currency": random.choice(CURRENCIES),
                "customer_id": generate_customer_id(random.randint(CUSTOMER_ID_START, CUSTOMER_ID_END)),
            }

    valid_order_ids = list(order_lookup.keys())
    
    for i in range(1, num_events + 1):
        event_id = generate_event_id()
        order_id = random.choice(valid_order_ids)
        
        # Get order details
        order_details = order_lookup.get(order_id, {
            "amount": round(random.uniform(20.0, 5000.0), 2),
            "currency": random.choice(CURRENCIES),
        })
        customer_id = order_details.get("customer_id") or generate_customer_id(
            random.randint(CUSTOMER_ID_START, CUSTOMER_ID_END)
        )
        
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


def generate_order_updates(
    orders: List[Dict[str, Any]],
    update_rate: float = 0.05,
    days_back: int = 30,
) -> List[Dict[str, Any]]:
    """Generate incremental order updates to simulate CDC/late arriving data."""
    updates: List[Dict[str, Any]] = []
    if not orders:
        return updates

    update_count = int(len(orders) * update_rate)
    window_start = NOW - timedelta(days=days_back)
    for _ in range(update_count):
        base = random.choice(orders)
        updated = dict(base)
        updated_time = random_datetime(window_start, NOW)
        updated["updated_at"] = updated_time
        updated["order_timestamp"] = updated_time
        updated["status"] = random.choice(["COMPLETED", "SHIPPED", "CANCELLED"])
        updated["ingestion_timestamp"] = NOW.strftime("%Y-%m-%d %H:%M:%S.%f")
        updates.append(updated)

    return updates


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


def _date_key(value: str) -> str:
    if not value:
        return ""
    return value.split(" ")[0]


def write_daily_batches(
    base_dir: Path,
    data: List[Dict[str, Any]],
    date_field: str,
    prefix: str,
    days_back: int = 30,
):
    """Write daily batch CSV files partitioned by date."""
    base_dir.mkdir(parents=True, exist_ok=True)
    cutoff = (NOW - timedelta(days=days_back)).date()

    buckets: Dict[str, List[Dict[str, Any]]] = {}
    for row in data:
        date_value = _date_key(str(row.get(date_field, "")))
        if not date_value:
            continue
        try:
            d = datetime.fromisoformat(date_value).date()
        except ValueError:
            continue
        if d < cutoff:
            continue
        buckets.setdefault(date_value, []).append(row)

    for date_value, rows in buckets.items():
        target_dir = base_dir / f"date={date_value}"
        write_csv(target_dir / f"{prefix}_{date_value}.csv", rows, list(rows[0].keys()))


def main():
    """Main function to regenerate all source data."""
    import argparse

    parser = argparse.ArgumentParser(description="Regenerate synthetic source data for Project A")
    parser.add_argument("--daily-days", type=int, default=30, help="Number of days of daily batches")
    parser.add_argument("--skip-bronze", action="store_true", help="Skip writing to data/bronze")
    parser.add_argument("--skip-aws-samples", action="store_true", help="Skip writing to aws/data/samples")
    args = parser.parse_args()

    print("🔄 Regenerating all source data files...")
    print("=" * 80)
    
    # CRM data
    print("\n📊 Generating CRM data...")
    accounts = generate_crm_accounts(30153)
    if not args.skip_aws_samples:
        write_csv(
            SAMPLES_DIR / "crm" / "accounts.csv",
            accounts,
            list(accounts[0].keys())
        )
    if not args.skip_bronze:
        write_csv(
            BRONZE_DIR / "crm" / "accounts.csv",
            accounts,
            list(accounts[0].keys())
        )
    
    contacts = generate_crm_contacts(45000)
    if not args.skip_aws_samples:
        write_csv(
            SAMPLES_DIR / "crm" / "contacts.csv",
            contacts,
            list(contacts[0].keys())
        )
    if not args.skip_bronze:
        write_csv(
            BRONZE_DIR / "crm" / "contacts.csv",
            contacts,
            list(contacts[0].keys())
        )
    
    opportunities = generate_crm_opportunities(20000)
    if not args.skip_aws_samples:
        write_csv(
            SAMPLES_DIR / "crm" / "opportunities.csv",
            opportunities,
            list(opportunities[0].keys())
        )
    if not args.skip_bronze:
        write_csv(
            BRONZE_DIR / "crm" / "opportunities.csv",
            opportunities,
            list(opportunities[0].keys())
        )
    
    # Snowflake data
    print("\n📊 Generating Snowflake data...")
    customers = generate_snowflake_customers(50000)
    if not args.skip_aws_samples:
        write_csv(
            SAMPLES_DIR / "snowflake" / "snowflake_customers_50000.csv",
            customers,
            list(customers[0].keys())
        )
    if not args.skip_bronze:
        write_csv(
            BRONZE_DIR / "snowflake" / "snowflake_customers_50000.csv",
            customers,
            list(customers[0].keys())
        )
    
    products = generate_snowflake_products(10000)
    if not args.skip_aws_samples:
        write_csv(
            SAMPLES_DIR / "snowflake" / "snowflake_products_10000.csv",
            products,
            list(products[0].keys())
        )
    if not args.skip_bronze:
        write_csv(
            BRONZE_DIR / "snowflake" / "snowflake_products_10000.csv",
            products,
            list(products[0].keys())
        )
    
    orders = generate_snowflake_orders(100000)
    if not args.skip_aws_samples:
        write_csv(
            SAMPLES_DIR / "snowflake" / "snowflake_orders_100000.csv",
            orders,
            list(orders[0].keys())
        )
    if not args.skip_bronze:
        write_csv(
            BRONZE_DIR / "snowflake" / "snowflake_orders_100000.csv",
            orders,
            list(orders[0].keys())
        )
    
    # Redshift data
    print("\n📊 Generating Redshift behavior data...")
    behavior = generate_redshift_behavior(50000)
    if not args.skip_aws_samples:
        write_csv(
            SAMPLES_DIR / "redshift" / "redshift_customer_behavior_50000.csv",
            behavior,
            list(behavior[0].keys())
        )
    if not args.skip_bronze:
        write_csv(
            BRONZE_DIR / "redshift" / "redshift_customer_behavior_50000.csv",
            behavior,
            list(behavior[0].keys())
        )
    
    # Kafka data
    print("\n📊 Generating Kafka events data...")
    kafka_events = generate_kafka_events(100000, orders=orders)
    if not args.skip_aws_samples:
        write_csv(
            SAMPLES_DIR / "kafka" / "stream_kafka_events_100000.csv",
            kafka_events,
            list(kafka_events[0].keys())
        )
    if not args.skip_bronze:
        write_csv(
            BRONZE_DIR / "kafka" / "stream_kafka_events_100000.csv",
            kafka_events,
            list(kafka_events[0].keys())
        )
    
    # FX rates
    print("\n📊 Generating FX rates data...")
    fx_rates_csv = generate_fx_rates_csv(730)
    if not args.skip_aws_samples:
        write_csv(
            SAMPLES_DIR / "fx" / "fx_rates_historical_730_days.csv",
            fx_rates_csv,
            ["trade_date", "base_ccy", "quote_ccy", "fx_rate"]
        )
    if not args.skip_bronze:
        write_csv(
            BRONZE_DIR / "fx" / "fx_rates_historical_730_days.csv",
            fx_rates_csv,
            ["trade_date", "base_ccy", "quote_ccy", "fx_rate"]
        )
    
    fx_rates_json = generate_fx_rates_json(730)
    if not args.skip_aws_samples:
        write_json_lines(
            SAMPLES_DIR / "fx" / "fx_rates_historical.json",
            fx_rates_json
        )
    if not args.skip_bronze:
        write_json_lines(
            BRONZE_DIR / "fx" / "json" / "fx_rates_historical.json",
            fx_rates_json
        )

    if not args.skip_bronze:
        print("\n📊 Writing daily incremental batches...")
        order_updates = generate_order_updates(orders, update_rate=0.05, days_back=args.daily_days)
        daily_orders = orders + order_updates
        # Orders daily batches (use updated_at for CDC realism)
        write_daily_batches(
            BRONZE_DIR / "snowflake" / "orders" / "daily",
            daily_orders,
            "updated_at",
            "orders",
            days_back=args.daily_days,
        )
        # Behavior daily batches
        write_daily_batches(
            BRONZE_DIR / "redshift" / "behavior" / "daily",
            behavior,
            "event_timestamp",
            "behavior",
            days_back=args.daily_days,
        )
        # Kafka daily batches
        write_daily_batches(
            BRONZE_DIR / "kafka" / "events" / "daily",
            kafka_events,
            "timestamp",
            "kafka_events",
            days_back=args.daily_days,
        )

    print("\n" + "=" * 80)
    print("✅ All source data files regenerated successfully!")
    print("\nNext steps:")
    print("1. Run: python jobs/transform/bronze_to_silver.py --env local --config local/config/local.yaml")
    print("2. Run: python jobs/transform/silver_to_gold.py --env local --config local/config/local.yaml")
    print("3. Run: python tools/validate_local_etl.py --env local --config local/config/local.yaml")


if __name__ == "__main__":
    main()
