#!/usr/bin/env python3
"""
Generate realistic, aligned sample data for local pipelines.

This script creates snapshot CSVs plus incremental daily batches for:
- CRM (accounts, contacts, opportunities)
- Snowflake (customers, orders, products)
- Redshift (behavior)
- Kafka (order events)
- FX (JSON lines)

All IDs are aligned across sources (customer_id used consistently),
and order currencies are covered by FX rates.
"""

from __future__ import annotations

import argparse
import json
import shutil
import random
from datetime import date, datetime, time, timedelta
from pathlib import Path
from typing import Iterable

import numpy as np
import pandas as pd
from faker import Faker

from project_a.schemas.bronze_schemas import (
    CRM_ACCOUNTS_SCHEMA,
    CRM_CONTACTS_SCHEMA,
    CRM_OPPORTUNITIES_SCHEMA,
    KAFKA_EVENTS_SCHEMA,
    REDSHIFT_BEHAVIOR_SCHEMA,
    SNOWFLAKE_CUSTOMERS_SCHEMA,
    SNOWFLAKE_ORDERS_SCHEMA,
    SNOWFLAKE_PRODUCTS_SCHEMA,
)


def _ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def _date_range(start: date, end: date) -> list[date]:
    days = (end - start).days
    return [start + timedelta(days=i) for i in range(days + 1)]


def _random_dates(rng: np.random.Generator, start: date, end: date, n: int) -> list[date]:
    days = (end - start).days
    offsets = rng.integers(0, max(days, 1), size=n)
    return [start + timedelta(days=int(i)) for i in offsets]


def _random_timestamp(rng: np.random.Generator, d: date) -> str:
    seconds = int(rng.integers(0, 24 * 3600))
    ts = datetime.combine(d, time.min) + timedelta(seconds=seconds)
    return ts.strftime("%Y-%m-%d %H:%M:%S")


def _write_csv(df: pd.DataFrame, path: Path) -> None:
    _ensure_dir(path.parent)
    df.to_csv(path, index=False)


def _write_json_lines(rows: Iterable[dict], path: Path) -> None:
    _ensure_dir(path.parent)
    with path.open("w", encoding="utf-8") as f:
        for row in rows:
            f.write(json.dumps(row) + "\n")


def _schema_fields(schema) -> list[str]:
    return [f.name for f in schema.fields]


def _weighted_customer_ids(rng: np.random.Generator, customer_ids: list[str], n: int) -> list[str]:
    # Zipf-like distribution for long-tail activity
    ranks = rng.zipf(a=1.3, size=n)
    ranks = np.clip(ranks, 1, len(customer_ids))
    return [customer_ids[int(r) - 1] for r in ranks]


def generate_data(
    output_base: Path,
    n_customers: int,
    n_products: int,
    n_orders: int,
    n_accounts: int,
    n_contacts: int,
    n_opps: int,
    n_behavior: int,
    n_events: int,
    start_date: date,
    end_date: date,
    increment_days: int,
    new_orders_per_day: int,
    updates_per_day: int,
    deletes_per_day: int,
) -> None:
    faker = Faker("en_US")
    Faker.seed(42)
    rng = np.random.default_rng(42)
    random.seed(42)

    currencies = ["USD", "EUR", "GBP", "JPY", "INR", "CHF", "CAD", "AUD", "CNY"]
    segments = ["consumer", "vip", "business", "enterprise"]
    channels = ["web", "mobile", "store", "partner"]
    order_statuses = ["shipped", "delivered", "processing", "cancelled", "returned"]
    devices = ["mobile", "desktop", "tablet"]
    browsers = ["chrome", "safari", "edge", "firefox"]
    event_types = ["order_created", "order_paid", "order_shipped", "order_delivered", "refund"]
    industries = ["technology", "finance", "retail", "healthcare", "manufacturing"]

    # Core IDs
    customer_ids = [f"CUST{idx:06d}" for idx in range(1, n_customers + 1)]
    product_ids = [f"PROD{idx:06d}" for idx in range(1, n_products + 1)]
    order_ids = [f"ORD{idx:08d}" for idx in range(1, n_orders + 1)]

    # Reset incremental directories to avoid double-counting
    for daily_dir in [
        output_base / "snowflake" / "orders" / "daily",
        output_base / "redshift" / "behavior" / "daily",
        output_base / "kafka" / "events" / "daily",
        output_base / "crm" / "opportunities" / "daily",
    ]:
        if daily_dir.exists():
            shutil.rmtree(daily_dir)

    # Generate FX rates
    fx_rows = []
    fx_dates = _date_range(start_date, end_date)
    base_rates = {
        "USD": 1.0,
        "EUR": 1.08,
        "GBP": 1.27,
        "JPY": 0.0070,
        "INR": 0.012,
        "CHF": 1.12,
        "CAD": 0.75,
        "AUD": 0.67,
        "CNY": 0.14,
    }
    for d in fx_dates:
        for ccy in currencies:
            base = base_rates[ccy]
            # Small daily noise
            rate = base * (1 + rng.normal(0, 0.003))
            rate = max(rate, 0.0001)
            fx_rows.append(
                {
                    "date": d.isoformat(),
                    "base_ccy": ccy,
                    "quote_ccy": "USD",
                    "rate": round(rate, 6),
                    "source": "synthetic",
                    "bid_rate": round(rate * 0.999, 6),
                    "ask_rate": round(rate * 1.001, 6),
                    "mid_rate": round(rate, 6),
                }
            )

    fx_dir = output_base / "fx" / "json"
    _write_json_lines(fx_rows, fx_dir / "fx_rates_historical.json")

    fx_lookup = {(row["date"], row["base_ccy"]): row["rate"] for row in fx_rows}

    # Products
    products = []
    for pid in product_ids:
        price = round(float(rng.uniform(5, 500)), 2)
        cost = round(price * rng.uniform(0.4, 0.7), 2)
        products.append(
            {
                "product_id": pid,
                "sku": f"SKU-{pid[-6:]}",
                "product_name": faker.word().title(),
                "category": random.choice(["electronics", "apparel", "home", "sports"]),
                "subcategory": random.choice(["basic", "premium", "deluxe"]),
                "brand": faker.company(),
                "price_usd": price,
                "cost_usd": cost,
                "weight_kg": round(float(rng.uniform(0.1, 10.0)), 2),
                "dimensions": f"{rng.integers(5,50)}x{rng.integers(5,50)}x{rng.integers(1,20)}",
                "color": random.choice(["black", "white", "red", "blue", "green"]),
                "size": random.choice(["S", "M", "L", "XL"]),
                "active": True,
                "stock_quantity": int(rng.integers(0, 500)),
                "reorder_level": int(rng.integers(10, 50)),
                "supplier_id": f"SUP{rng.integers(1, 200):04d}",
                "created_at": _random_timestamp(rng, start_date),
                "updated_at": _random_timestamp(rng, end_date),
                "source_system": "snowflake",
                "ingestion_timestamp": datetime.utcnow().isoformat(),
                "rating": round(float(rng.uniform(2.5, 5.0)), 2),
                "review_count": int(rng.integers(0, 5000)),
                "in_stock": True,
            }
        )

    products_df = pd.DataFrame(products, columns=_schema_fields(SNOWFLAKE_PRODUCTS_SCHEMA))
    _write_csv(products_df, output_base / "snowflake" / "snowflake_products_10000.csv")

    # Customers
    customers = []
    for idx, cid in enumerate(customer_ids, start=1):
        first = faker.first_name()
        last = faker.last_name()
        acct_domain = f"acct{(idx % 5000) + 1}.com"
        reg_date = _random_dates(rng, start_date, end_date - timedelta(days=30), 1)[0]
        created_ts = _random_timestamp(rng, reg_date)
        updated_ts = _random_timestamp(rng, min(end_date, reg_date + timedelta(days=180)))
        customers.append(
            {
                "customer_id": cid,
                "first_name": first,
                "last_name": last,
                "email": f"{first.lower()}.{last.lower()}@{acct_domain}",
                "phone": faker.phone_number(),
                "address": faker.street_address(),
                "city": faker.city(),
                "state": faker.state_abbr(),
                "country": "USA",
                "zip_code": faker.postcode(),
                "registration_date": reg_date.isoformat(),
                "gender": random.choice(["M", "F", "O"]),
                "age": int(rng.integers(18, 75)),
                "customer_segment": random.choice(segments),
                "lifetime_value": round(float(rng.lognormal(3.5, 0.5)), 2),
                "last_purchase_date": None,  # filled after orders
                "total_orders": None,  # filled after orders
                "preferred_channel": random.choice(channels),
                "created_at": created_ts,
                "updated_at": updated_ts,
                "source_system": "snowflake",
                "ingestion_timestamp": datetime.utcnow().isoformat(),
                "estimated_clv": round(float(rng.lognormal(3.2, 0.6)), 2),
            }
        )

    # Orders
    product_price = {p["product_id"]: p["price_usd"] for p in products}
    order_customers = _weighted_customer_ids(rng, customer_ids, n_orders)
    order_products = rng.choice(product_ids, size=n_orders)
    order_dates = _random_dates(rng, start_date, end_date, n_orders)

    orders = []
    for i, oid in enumerate(order_ids):
        cid = order_customers[i]
        pid = order_products[i]
        odate = order_dates[i]
        currency = random.choice(currencies)
        qty = int(rng.integers(1, 5))
        unit_price_usd = product_price[pid] * float(rng.uniform(0.95, 1.1))
        amount_usd = unit_price_usd * qty
        fx_rate = fx_lookup.get((odate.isoformat(), currency), 1.0)
        total_amount = round(amount_usd / fx_rate, 2)
        order_ts = _random_timestamp(rng, odate)
        orders.append(
            {
                "order_id": oid,
                "customer_id": cid,
                "product_id": pid,
                "order_date": odate.isoformat(),
                "order_timestamp": order_ts,
                "quantity": qty,
                "unit_price": round(unit_price_usd, 2),
                "total_amount": total_amount,
                "currency": currency,
                "payment_method": random.choice(["card", "paypal", "bank_transfer"]),
                "shipping_method": random.choice(["standard", "express"]),
                "status": random.choice(order_statuses),
                "op": "INSERT",
                "is_deleted": False,
                "shipping_address": faker.street_address(),
                "billing_address": faker.street_address(),
                "discount_percent": round(float(rng.uniform(0, 20)), 2),
                "tax_amount": round(total_amount * 0.08, 2),
                "shipping_cost": round(float(rng.uniform(0, 25)), 2),
                "promo_code": random.choice(["", "SPRING", "VIP", "WELCOME"]),
                "sales_rep_id": f"SR{rng.integers(1, 300):04d}",
                "channel": random.choice(channels),
                "created_at": order_ts,
                "updated_at": order_ts,
                "source_system": "snowflake",
                "ingestion_timestamp": datetime.utcnow().isoformat(),
                "order_month": odate.month,
                "order_quarter": int((odate.month - 1) / 3) + 1,
                "order_year": odate.year,
                "order_segment": random.choice(segments),
                "customer_order_count": None,
                "fulfillment_days": int(rng.integers(1, 10)),
            }
        )

    orders_df = pd.DataFrame(orders, columns=_schema_fields(SNOWFLAKE_ORDERS_SCHEMA))
    _write_csv(orders_df, output_base / "snowflake" / "snowflake_orders_100000.csv")

    # Backfill customer aggregates
    orders_group = orders_df.groupby("customer_id").agg(
        total_orders=("order_id", "count"),
        last_purchase_date=("order_date", "max"),
    )
    customer_df = pd.DataFrame(customers, columns=_schema_fields(SNOWFLAKE_CUSTOMERS_SCHEMA))
    # Replace placeholder aggregates with actuals
    customer_df = customer_df.drop(columns=["total_orders", "last_purchase_date"], errors="ignore")
    customer_df = customer_df.merge(orders_group, on="customer_id", how="left")
    customer_df["total_orders"] = customer_df["total_orders"].fillna(0).astype(int)
    customer_df["last_purchase_date"] = customer_df["last_purchase_date"].fillna(
        customer_df["registration_date"]
    )
    customer_df = customer_df[_schema_fields(SNOWFLAKE_CUSTOMERS_SCHEMA)]
    _write_csv(customer_df, output_base / "snowflake" / "snowflake_customers_50000.csv")

    # CRM accounts aligned to customer_id
    accounts = []
    for idx, cid in enumerate(customer_ids, start=1):
        created = _random_dates(rng, start_date, end_date - timedelta(days=180), 1)[0]
        accounts.append(
            {
                "Id": cid,
                "Name": faker.company(),
                "Phone": faker.phone_number(),
                "Website": f"https://acct{(idx % 5000) + 1}.com",
                "Industry": random.choice(industries),
                "AnnualRevenue": round(float(rng.lognormal(5.0, 0.7)), 2),
                "NumberOfEmployees": int(rng.integers(10, 20000)),
                "BillingStreet": faker.street_address(),
                "BillingCity": faker.city(),
                "BillingState": faker.state_abbr(),
                "BillingPostalCode": faker.postcode(),
                "BillingCountry": "USA",
                "Rating": random.choice(["Hot", "Warm", "Cold"]),
                "Type": random.choice(["Customer", "Partner", "Prospect"]),
                "AccountSource": random.choice(["web", "partner", "referral"]),
                "AccountNumber": f"ACCT{idx:06d}",
                "Site": random.choice(["US", "EMEA", "APAC"]),
                "Description": faker.catch_phrase(),
                "Ownership": random.choice(["Public", "Private"]),
                "ParentId": "",
                "TickerSymbol": "",
                "YearStarted": int(rng.integers(1980, 2020)),
                "CreatedDate": created.isoformat(),
                "LastModifiedDate": (created + timedelta(days=int(rng.integers(0, 365)))).isoformat(),
                "OwnerId": f"OWN{rng.integers(1, 300):04d}",
                "CustomerSegment": random.choice(segments),
                "GeographicRegion": random.choice(["NA", "EMEA", "APAC", "LATAM"]),
                "AccountStatus": random.choice(["active", "on_hold", "churned"]),
                "LastActivityDate": (created + timedelta(days=int(rng.integers(1, 365)))).isoformat(),
            }
        )

    accounts_df = pd.DataFrame(accounts, columns=_schema_fields(CRM_ACCOUNTS_SCHEMA))
    _write_csv(accounts_df, output_base / "crm" / "accounts.csv")

    # CRM contacts (1 per account)
    contacts = []
    for idx, cid in enumerate(customer_ids[:n_contacts], start=1):
        first = faker.first_name()
        last = faker.last_name()
        created = _random_dates(rng, start_date, end_date - timedelta(days=90), 1)[0]
        contacts.append(
            {
                "Id": f"CON{idx:07d}",
                "LastName": last,
                "AccountId": cid,
                "FirstName": first,
                "Email": f"{first.lower()}.{last.lower()}@acct{(idx % 5000) + 1}.com",
                "Phone": faker.phone_number(),
                "MobilePhone": faker.phone_number(),
                "Title": random.choice(["Manager", "Director", "VP", "Analyst"]),
                "Department": random.choice(["Sales", "Marketing", "IT", "Finance"]),
                "LeadSource": random.choice(["web", "partner", "event"]),
                "MailingStreet": faker.street_address(),
                "MailingCity": faker.city(),
                "MailingState": faker.state_abbr(),
                "MailingPostalCode": faker.postcode(),
                "MailingCountry": "USA",
                "DoNotCall": False,
                "HasOptedOutOfEmail": False,
                "Description": "",
                "CreatedDate": created.isoformat(),
                "LastModifiedDate": (created + timedelta(days=int(rng.integers(1, 180)))).isoformat(),
                "OwnerId": f"OWN{rng.integers(1, 300):04d}",
                "ContactRole": random.choice(["Primary", "Billing", "Technical"]),
                "ContactLevel": random.choice(["L1", "L2", "L3"]),
                "EngagementScore": round(float(rng.uniform(0, 100)), 2),
            }
        )

    contacts_df = pd.DataFrame(contacts, columns=_schema_fields(CRM_CONTACTS_SCHEMA))
    _write_csv(contacts_df, output_base / "crm" / "contacts.csv")

    # CRM opportunities
    opps = []
    for idx in range(1, n_opps + 1):
        account_id = random.choice(customer_ids)
        contact_id = f"CON{rng.integers(1, n_contacts + 1):07d}"
        close_dt = _random_dates(rng, start_date, end_date, 1)[0]
        stage = random.choice(["Prospecting", "Qualification", "Proposal", "Closed Won", "Closed Lost"])
        is_won = stage == "Closed Won"
        is_closed = stage in ("Closed Won", "Closed Lost")
        created = close_dt - timedelta(days=int(rng.integers(10, 120)))
        opps.append(
            {
                "Id": f"OPP{idx:07d}",
                "Name": f"Opportunity {idx}",
                "AccountId": account_id,
                "StageName": stage,
                "CloseDate": close_dt.isoformat(),
                "Amount": round(float(rng.lognormal(4.5, 0.6)), 2),
                "Probability": int(rng.integers(10, 95)),
                "LeadSource": random.choice(["web", "partner", "event"]),
                "Type": random.choice(["New", "Upsell", "Renewal"]),
                "NextStep": "Follow-up",
                "Description": "",
                "ForecastCategory": random.choice(["Pipeline", "BestCase", "Commit"]),
                "IsClosed": is_closed,
                "IsWon": is_won,
                "CreatedDate": created.isoformat(),
                "LastModifiedDate": close_dt.isoformat(),
                "OwnerId": f"OWN{rng.integers(1, 300):04d}",
                "DealSize": random.choice(["Small", "Medium", "Large"]),
                "SalesCycle": int(rng.integers(15, 120)),
                "ProductInterest": random.choice(["core", "add-on", "bundle"]),
                "Budget": random.choice(["low", "medium", "high"]),
                "Timeline": random.choice(["short", "medium", "long"]),
            }
        )

    opps_df = pd.DataFrame(opps, columns=_schema_fields(CRM_OPPORTUNITIES_SCHEMA))
    _write_csv(opps_df, output_base / "crm" / "opportunities.csv")

    # Redshift behavior
    behavior = []
    behavior_ids = [f"BEH{idx:07d}" for idx in range(1, n_behavior + 1)]
    behavior_customers = _weighted_customer_ids(rng, customer_ids, n_behavior)
    behavior_dates = _random_dates(rng, start_date, end_date, n_behavior)
    for idx in range(n_behavior):
        cid = behavior_customers[idx]
        d = behavior_dates[idx]
        behavior.append(
            {
                "behavior_id": behavior_ids[idx],
                "customer_id": cid,
                "event_name": random.choice(["page_view", "search", "add_to_cart", "checkout"]),
                "event_timestamp": _random_timestamp(rng, d),
                "session_id": f"S{rng.integers(1, 500000):08d}",
                "page_url": f"/product/{rng.integers(1, 10000)}",
                "referrer": random.choice(["google", "bing", "direct", "email"]),
                "device_type": random.choice(devices),
                "browser": random.choice(browsers),
                "os": random.choice(["ios", "android", "windows", "mac"]),
                "country": "USA",
                "city": faker.city(),
                "user_agent": "Mozilla/5.0",
                "ip_address": faker.ipv4(),
                "duration_seconds": int(rng.integers(5, 600)),
                "conversion_value": round(float(rng.uniform(0, 200)), 2),
                "utm_source": random.choice(["google", "facebook", "linkedin", "email"]),
                "utm_medium": random.choice(["cpc", "organic", "referral"]),
                "utm_campaign": random.choice(["spring", "summer", "fall", "winter"]),
                "behavior_segment": random.choice(segments),
                "conversion_probability": round(float(rng.uniform(0, 1)), 2),
                "journey_stage": random.choice(["awareness", "consideration", "purchase"]),
                "engagement_score": round(float(rng.uniform(0, 100)), 2),
                "session_quality": random.choice(["low", "medium", "high"]),
            }
        )

    behavior_df = pd.DataFrame(behavior, columns=_schema_fields(REDSHIFT_BEHAVIOR_SCHEMA))
    _write_csv(behavior_df, output_base / "redshift" / "redshift_customer_behavior_50000.csv")

    # Kafka events
    events = []
    event_ids = [f"EVT{idx:07d}" for idx in range(1, n_events + 1)]
    event_orders = rng.choice(order_ids, size=n_events)
    order_lookup = orders_df.set_index("order_id")[["customer_id", "currency", "total_amount"]]
    for idx, eid in enumerate(event_ids):
        oid = event_orders[idx]
        row = order_lookup.loc[oid]
        d = _random_dates(rng, start_date, end_date, 1)[0]
        session_id = f"S{rng.integers(1, 500000):08d}"
        value = {
            "customer_id": row["customer_id"],
            "event_type": random.choice(event_types),
            "amount": float(row["total_amount"]),
            "currency": row["currency"],
            "order_id": oid,
            "metadata": {"source": "kafka", "version": "1.0", "session_id": session_id},
        }
        headers = {"customer_id": row["customer_id"]}
        events.append(
            {
                "event_id": eid,
                "topic": "orders_events",
                "partition": int(rng.integers(0, 8)),
                "offset": int(rng.integers(1, 1_000_000)),
                "timestamp": _random_timestamp(rng, d),
                "key": row["customer_id"],
                "value": json.dumps(value),
                "headers": json.dumps(headers),
            }
        )

    events_df = pd.DataFrame(events, columns=_schema_fields(KAFKA_EVENTS_SCHEMA))
    _write_csv(events_df, output_base / "kafka" / "stream_kafka_events_100000.csv")

    # Incremental batches
    inc_dates = _date_range(end_date - timedelta(days=increment_days - 1), end_date)

    # Orders incremental
    daily_orders_dir = output_base / "snowflake" / "orders" / "daily"
    next_order_idx = n_orders + 1
    for d in inc_dates:
        daily = []
        # New orders
        for _ in range(new_orders_per_day):
            oid = f"ORD{next_order_idx:08d}"
            next_order_idx += 1
            cid = random.choice(customer_ids)
            pid = random.choice(product_ids)
            currency = random.choice(currencies)
            qty = int(rng.integers(1, 4))
            unit_price_usd = product_price[pid]
            amount_usd = unit_price_usd * qty
            fx_rate = fx_lookup.get((d.isoformat(), currency), 1.0)
            total_amount = round(amount_usd / fx_rate, 2)
            ts = _random_timestamp(rng, d)
            daily.append(
                {
                    "order_id": oid,
                    "customer_id": cid,
                    "product_id": pid,
                    "order_date": d.isoformat(),
                    "order_timestamp": ts,
                    "quantity": qty,
                    "unit_price": round(unit_price_usd, 2),
                    "total_amount": total_amount,
                    "currency": currency,
                    "payment_method": random.choice(["card", "paypal", "bank_transfer"]),
                    "shipping_method": random.choice(["standard", "express"]),
                    "status": random.choice(order_statuses),
                    "op": "INSERT",
                    "is_deleted": False,
                    "shipping_address": faker.street_address(),
                    "billing_address": faker.street_address(),
                    "discount_percent": round(float(rng.uniform(0, 20)), 2),
                    "tax_amount": round(total_amount * 0.08, 2),
                    "shipping_cost": round(float(rng.uniform(0, 25)), 2),
                    "promo_code": random.choice(["", "SPRING", "VIP", "WELCOME"]),
                    "sales_rep_id": f"SR{rng.integers(1, 300):04d}",
                    "channel": random.choice(channels),
                    "created_at": ts,
                    "updated_at": ts,
                    "source_system": "snowflake",
                    "ingestion_timestamp": datetime.utcnow().isoformat(),
                    "order_month": d.month,
                    "order_quarter": int((d.month - 1) / 3) + 1,
                    "order_year": d.year,
                    "order_segment": random.choice(segments),
                    "customer_order_count": None,
                    "fulfillment_days": int(rng.integers(1, 10)),
                }
            )

        # Updates
        for _ in range(updates_per_day):
            target = random.choice(order_ids)
            update_ts = _random_timestamp(rng, d)
            daily.append(
                {
                    "order_id": target,
                    "customer_id": orders_df.loc[orders_df["order_id"] == target, "customer_id"].values[0],
                    "product_id": orders_df.loc[orders_df["order_id"] == target, "product_id"].values[0],
                    "order_date": d.isoformat(),
                    "order_timestamp": update_ts,
                    "quantity": int(rng.integers(1, 4)),
                    "unit_price": float(orders_df.loc[orders_df["order_id"] == target, "unit_price"].values[0]),
                    "total_amount": float(orders_df.loc[orders_df["order_id"] == target, "total_amount"].values[0]),
                    "currency": orders_df.loc[orders_df["order_id"] == target, "currency"].values[0],
                    "payment_method": random.choice(["card", "paypal", "bank_transfer"]),
                    "shipping_method": random.choice(["standard", "express"]),
                    "status": random.choice(order_statuses),
                    "op": "UPDATE",
                    "is_deleted": False,
                    "shipping_address": faker.street_address(),
                    "billing_address": faker.street_address(),
                    "discount_percent": round(float(rng.uniform(0, 20)), 2),
                    "tax_amount": round(float(rng.uniform(0, 20)), 2),
                    "shipping_cost": round(float(rng.uniform(0, 25)), 2),
                    "promo_code": random.choice(["", "SPRING", "VIP", "WELCOME"]),
                    "sales_rep_id": f"SR{rng.integers(1, 300):04d}",
                    "channel": random.choice(channels),
                    "created_at": update_ts,
                    "updated_at": update_ts,
                    "source_system": "snowflake",
                    "ingestion_timestamp": datetime.utcnow().isoformat(),
                    "order_month": d.month,
                    "order_quarter": int((d.month - 1) / 3) + 1,
                    "order_year": d.year,
                    "order_segment": random.choice(segments),
                    "customer_order_count": None,
                    "fulfillment_days": int(rng.integers(1, 10)),
                }
            )

        # Deletes
        for _ in range(deletes_per_day):
            target = random.choice(order_ids)
            delete_ts = _random_timestamp(rng, d)
            daily.append(
                {
                    "order_id": target,
                    "customer_id": orders_df.loc[orders_df["order_id"] == target, "customer_id"].values[0],
                    "product_id": orders_df.loc[orders_df["order_id"] == target, "product_id"].values[0],
                    "order_date": d.isoformat(),
                    "order_timestamp": delete_ts,
                    "quantity": None,
                    "unit_price": None,
                    "total_amount": None,
                    "currency": orders_df.loc[orders_df["order_id"] == target, "currency"].values[0],
                    "payment_method": None,
                    "shipping_method": None,
                    "status": "cancelled",
                    "op": "DELETE",
                    "is_deleted": True,
                    "shipping_address": None,
                    "billing_address": None,
                    "discount_percent": None,
                    "tax_amount": None,
                    "shipping_cost": None,
                    "promo_code": None,
                    "sales_rep_id": None,
                    "channel": None,
                    "created_at": delete_ts,
                    "updated_at": delete_ts,
                    "source_system": "snowflake",
                    "ingestion_timestamp": datetime.utcnow().isoformat(),
                    "order_month": d.month,
                    "order_quarter": int((d.month - 1) / 3) + 1,
                    "order_year": d.year,
                    "order_segment": None,
                    "customer_order_count": None,
                    "fulfillment_days": None,
                }
            )

        daily_df = pd.DataFrame(daily, columns=_schema_fields(SNOWFLAKE_ORDERS_SCHEMA))
        daily_path = daily_orders_dir / f"date={d.isoformat()}" / f"orders_{d.isoformat()}.csv"
        _write_csv(daily_df, daily_path)

    # Redshift behavior incremental
    daily_behavior_dir = output_base / "redshift" / "behavior" / "daily"
    for d in inc_dates:
        inc_rows = []
        for _ in range(int(n_behavior / 100)):
            cid = random.choice(customer_ids)
            inc_rows.append(
                {
                    "behavior_id": f"BEH{rng.integers(10_000_000, 99_999_999)}",
                    "customer_id": cid,
                    "event_name": random.choice(["page_view", "search", "add_to_cart", "checkout"]),
                    "event_timestamp": _random_timestamp(rng, d),
                    "session_id": f"S{rng.integers(1, 500000):08d}",
                    "page_url": f"/product/{rng.integers(1, 10000)}",
                    "referrer": random.choice(["google", "bing", "direct", "email"]),
                    "device_type": random.choice(devices),
                    "browser": random.choice(browsers),
                    "os": random.choice(["ios", "android", "windows", "mac"]),
                    "country": "USA",
                    "city": faker.city(),
                    "user_agent": "Mozilla/5.0",
                    "ip_address": faker.ipv4(),
                    "duration_seconds": int(rng.integers(5, 600)),
                    "conversion_value": round(float(rng.uniform(0, 200)), 2),
                    "utm_source": random.choice(["google", "facebook", "linkedin", "email"]),
                    "utm_medium": random.choice(["cpc", "organic", "referral"]),
                    "utm_campaign": random.choice(["spring", "summer", "fall", "winter"]),
                    "behavior_segment": random.choice(segments),
                    "conversion_probability": round(float(rng.uniform(0, 1)), 2),
                    "journey_stage": random.choice(["awareness", "consideration", "purchase"]),
                    "engagement_score": round(float(rng.uniform(0, 100)), 2),
                    "session_quality": random.choice(["low", "medium", "high"]),
                }
            )
        inc_df = pd.DataFrame(inc_rows, columns=_schema_fields(REDSHIFT_BEHAVIOR_SCHEMA))
        inc_path = daily_behavior_dir / f"date={d.isoformat()}" / f"behavior_{d.isoformat()}.csv"
        _write_csv(inc_df, inc_path)

    # Kafka events incremental
    daily_events_dir = output_base / "kafka" / "events" / "daily"
    for d in inc_dates:
        inc_events = []
        for _ in range(int(n_events / 100)):
            oid = random.choice(order_ids)
            row = order_lookup.loc[oid]
            session_id = f"S{rng.integers(1, 500000):08d}"
            value = {
                "customer_id": row["customer_id"],
                "event_type": random.choice(event_types),
                "amount": float(row["total_amount"]),
                "currency": row["currency"],
                "order_id": oid,
                "metadata": {"source": "kafka", "version": "1.0", "session_id": session_id},
            }
            headers = {"customer_id": row["customer_id"]}
            inc_events.append(
                {
                    "event_id": f"EVT{rng.integers(10_000_000, 99_999_999)}",
                    "topic": "orders_events",
                    "partition": int(rng.integers(0, 8)),
                    "offset": int(rng.integers(1, 1_000_000)),
                    "timestamp": _random_timestamp(rng, d),
                    "key": row["customer_id"],
                    "value": json.dumps(value),
                    "headers": json.dumps(headers),
                }
            )
        inc_df = pd.DataFrame(inc_events, columns=_schema_fields(KAFKA_EVENTS_SCHEMA))
        inc_path = daily_events_dir / f"date={d.isoformat()}" / f"events_{d.isoformat()}.csv"
        _write_csv(inc_df, inc_path)

    # CRM incremental (new opportunities)
    daily_opps_dir = output_base / "crm" / "opportunities" / "daily"
    for d in inc_dates:
        inc_opps = []
        for _ in range(int(n_opps / 100)):
            account_id = random.choice(customer_ids)
            close_dt = d + timedelta(days=int(rng.integers(10, 90)))
            stage = random.choice(["Prospecting", "Qualification", "Proposal", "Closed Won", "Closed Lost"])
            is_won = stage == "Closed Won"
            is_closed = stage in ("Closed Won", "Closed Lost")
            inc_opps.append(
                {
                    "Id": f"OPP{rng.integers(10_000_000, 99_999_999)}",
                    "Name": "Incremental Opportunity",
                    "AccountId": account_id,
                    "StageName": stage,
                    "CloseDate": close_dt.isoformat(),
                    "Amount": round(float(rng.lognormal(4.2, 0.5)), 2),
                    "Probability": int(rng.integers(10, 95)),
                    "LeadSource": random.choice(["web", "partner", "event"]),
                    "Type": random.choice(["New", "Upsell", "Renewal"]),
                    "NextStep": "Follow-up",
                    "Description": "",
                    "ForecastCategory": random.choice(["Pipeline", "BestCase", "Commit"]),
                    "IsClosed": is_closed,
                    "IsWon": is_won,
                    "CreatedDate": d.isoformat(),
                    "LastModifiedDate": d.isoformat(),
                    "OwnerId": f"OWN{rng.integers(1, 300):04d}",
                    "DealSize": random.choice(["Small", "Medium", "Large"]),
                    "SalesCycle": int(rng.integers(15, 120)),
                    "ProductInterest": random.choice(["core", "add-on", "bundle"]),
                    "Budget": random.choice(["low", "medium", "high"]),
                    "Timeline": random.choice(["short", "medium", "long"]),
                }
            )
        inc_df = pd.DataFrame(inc_opps, columns=_schema_fields(CRM_OPPORTUNITIES_SCHEMA))
        inc_path = daily_opps_dir / f"date={d.isoformat()}" / f"opportunities_{d.isoformat()}.csv"
        _write_csv(inc_df, inc_path)


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate realistic aligned sample data.")
    parser.add_argument("--output-base", default="data/bronze")
    parser.add_argument("--customers", type=int, default=50000)
    parser.add_argument("--products", type=int, default=10000)
    parser.add_argument("--orders", type=int, default=100000)
    parser.add_argument("--accounts", type=int, default=50000)
    parser.add_argument("--contacts", type=int, default=50000)
    parser.add_argument("--opps", type=int, default=20000)
    parser.add_argument("--behavior", type=int, default=50000)
    parser.add_argument("--events", type=int, default=100000)
    parser.add_argument("--start-date", default="2023-01-01")
    parser.add_argument("--end-date", default="2025-12-31")
    parser.add_argument("--increment-days", type=int, default=14)
    parser.add_argument("--new-orders-per-day", type=int, default=120)
    parser.add_argument("--updates-per-day", type=int, default=40)
    parser.add_argument("--deletes-per-day", type=int, default=10)
    args = parser.parse_args()

    output_base = Path(args.output_base)
    generate_data(
        output_base=output_base,
        n_customers=args.customers,
        n_products=args.products,
        n_orders=args.orders,
        n_accounts=args.accounts,
        n_contacts=args.contacts,
        n_opps=args.opps,
        n_behavior=args.behavior,
        n_events=args.events,
        start_date=date.fromisoformat(args.start_date),
        end_date=date.fromisoformat(args.end_date),
        increment_days=args.increment_days,
        new_orders_per_day=args.new_orders_per_day,
        updates_per_day=args.updates_per_day,
        deletes_per_day=args.deletes_per_day,
    )


if __name__ == "__main__":
    raise SystemExit(main())
