import csv
import json
import os
import random
from datetime import datetime, timedelta

try:
    from faker import Faker
except ImportError:
    raise SystemExit("Please install Faker: pip install Faker")


def mmddyy(d: datetime) -> str:
    return d.strftime("%m/%d/%y")


def generate_customers(path: str, num_rows: int = 2000, seed: int = 42) -> None:
    fake = Faker()
    Faker.seed(seed)
    random.seed(seed)
    states = [
        "AL","AK","AZ","AR","CA","CO","CT","DE","FL","GA","HI","ID","IL","IN","IA","KS","KY","LA","ME","MD","MA","MI","MN","MS","MO","MT","NE","NV","NH","NJ","NM","NY","NC","ND","OH","OK","OR","PA","RI","SC","SD","TN","TX","UT","VT","VA","WA","WV","WI","WY",
    ]
    countries = ["USA", "Canada", "UK", "Germany", "France", "India", "Japan", "Australia"]
    headers = [
        "customer_id","first_name","last_name","email","address","city","state","country","zip","phone","registration_date","gender","age"
    ]
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(headers)
        start_date = datetime.now() - timedelta(days=365 * 3)
        for i in range(1, num_rows + 1):
            gender = random.choice(["M", "F"])
            first = fake.first_name_male() if gender == "M" else fake.first_name_female()
            last = fake.last_name()
            email = f"{first.lower()}.{last.lower()}@{fake.free_email_domain()}"
            addr = fake.street_address()
            city = fake.city()
            state = random.choice(states)
            country = "USA" if random.random() < 0.85 else random.choice(countries)
            zipc = fake.postcode()
            phone = fake.phone_number()
            reg = mmddyy(start_date + timedelta(days=random.randint(0, 365 * 3)))
            age = random.randint(18, 80)
            w.writerow([f"C{i:05d}", first, last, email, addr, city, state, country, zipc, phone, reg, gender, age])


def generate_products(path: str, num_rows: int = 500, seed: int = 43) -> None:
    fake = Faker()
    Faker.seed(seed)
    random.seed(seed)
    categories = ["Electronics", "Beauty", "Clothing", "Grocery", "Home", "Sports"]
    brands = [fake.company() for _ in range(50)]
    headers = [
        "product_id","product_name","category","brand","price","in_stock","launch_date","rating","tags"
    ]
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(headers)
        start_date = datetime.now() - timedelta(days=365 * 5)
        for i in range(1, num_rows + 1):
            name = fake.catch_phrase()
            cat = random.choice(categories)
            brand = random.choice(brands)
            price = round(random.uniform(5, 2500), 2)
            in_stock = random.random() > 0.1
            launch = mmddyy(start_date + timedelta(days=random.randint(0, 365 * 5)))
            rating = round(random.uniform(2.0, 5.0), 1)
            tags = ", ".join(random.sample(["new", "promo", "eco", "lux", "hot", "refurb"], k=random.randint(0, 3)))
            w.writerow([f"P{i:05d}", name, cat, brand, price, str(in_stock).lower(), launch, rating, tags])


def generate_orders(path: str, customers_csv: str, products_csv: str, num_rows: int = 10000, seed: int = 44,
                    orphan_rate: float = 0.01, currency: bool = True) -> None:
    random.seed(seed)
    fake = Faker()
    Faker.seed(seed)
    # Load customer and product ids
    def read_ids(p: str, col: int) -> list[str]:
        with open(p, newline="") as f:
            r = csv.reader(f)
            next(r)
            return [row[col] for row in r]

    customer_ids = read_ids(customers_csv, 0)
    product_ids = read_ids(products_csv, 0)
    start_dt = datetime.now() - timedelta(days=365)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    rows = []
    currencies = ["USD", "EUR", "GBP", "INR"] if currency else ["USD"]
    for i in range(1, num_rows + 1):
        cust = random.choice(customer_ids) if random.random() > orphan_rate else f"C{99999:05d}"
        prod = random.choice(product_ids) if random.random() > orphan_rate else f"P{99999:05d}"
        order_dt = start_dt + timedelta(days=random.randint(0, 365), seconds=random.randint(0, 86400))
        qty = random.randint(1, 10)
        total = round(random.uniform(10, 20000), 2)
        shipping_addr = fake.street_address()
        shipping_city = fake.city()
        shipping_state = random.choice([
            "AL","AK","AZ","AR","CA","CO","CT","DE","FL","GA","HI","ID","IL","IN","IA","KS","KY","LA","ME","MD","MA","MI","MN","MS","MO","MT","NE","NV","NH","NJ","NM","NY","NC","ND","OH","OK","OR","PA","RI","SC","SD","TN","TX","UT","VT","VA","WA","WV","WI","WY",
        ])
        shipping_country = "USA"
        payment = {
            "method": random.choice(["card", "paypal", "apple_pay", "bank"]),
            "status": random.choice(["paid", "pending", "refunded"]),
        }
        rows.append({
            "order_id": f"O{i:06d}",
            "customer_id": cust,
            "product_id": prod,
            "order_date": order_dt.strftime("%Y-%m-%dT%H:%M:%S"),
            "quantity": qty,
            "total_amount": total,
            "currency": random.choice(currencies),
            "shipping_address": shipping_addr,
            "shipping_city": shipping_city,
            "shipping_state": shipping_state,
            "shipping_country": shipping_country,
            "payment": payment,
        })
def generate_returns(path: str, orders_json: str, rate: float = 0.05, seed: int = 45) -> None:
    random.seed(seed)
    with open(orders_json) as f:
        orders = json.load(f)
    returns = []
    for o in orders:
        if random.random() < rate:
            returns.append({
                "order_id": o["order_id"],
                "return_date": o["order_date"],
                "reason": random.choice(["damaged", "not_as_described", "changed_mind"])})
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w") as f:
        json.dump(returns, f, indent=2)


def generate_exchange_rates(path: str, seed: int = 46) -> None:
    random.seed(seed)
    rates = {
        "USD": 1.0,
        "EUR": round(random.uniform(1.05, 1.15), 3),
        "GBP": round(random.uniform(1.2, 1.35), 3),
        "INR": round(random.uniform(0.011, 0.014), 4),
    }
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["currency", "usd_rate"])
        for c, r in rates.items():
            w.writerow([c, r])


def generate_inventory_snapshots(path: str, products_csv: str, seed: int = 47) -> None:
    random.seed(seed)
    with open(products_csv, newline="") as f:
        r = csv.reader(f)
        next(r)
        rows = [row for row in r]
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["product_id", "on_hand", "warehouse"])
        for row in rows:
            w.writerow([row[0], random.randint(0, 1000), random.choice(["W1", "W2", "W3"])])


def generate_customer_changes(path: str, customers_csv: str, change_rate: float = 0.1, seed: int = 48) -> None:
    random.seed(seed)
    fake = Faker()
    with open(customers_csv, newline="") as f:
        r = csv.reader(f)
        headers = next(r)
        rows = [row for row in r]
    changes = []
    for row in rows:
        if random.random() < change_rate:
            cust_id = row[0]
            new_addr = fake.street_address()
            changes.append([cust_id, new_addr, datetime.now().strftime("%Y-%m-%d"), None])
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["customer_id", "new_address", "effective_from", "effective_to"])
        w.writerows(changes)

    with open(path, "w") as f:
        json.dump(rows, f, indent=2)


if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser(description="Generate realistic input datasets")
    ap.add_argument("--base", default=None, help="Base folder to write input files (default: data/input_data)")
    ap.add_argument("--customers", type=int, default=2000, help="Number of customers")
    ap.add_argument("--products", type=int, default=500, help="Number of products")
    ap.add_argument("--orders", type=int, default=10000, help="Number of orders")
    ap.add_argument("--orphan-rate", type=float, default=0.01)
    ap.add_argument("--return-rate", type=float, default=0.05)
    args = ap.parse_args()

    base = args.base or os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "data", "input_data"))
    customers_path = os.path.join(base, "customers.csv")
    products_path = os.path.join(base, "products.csv")
    orders_path = os.path.join(base, "orders.json")
    generate_customers(customers_path, num_rows=args.customers)
    generate_products(products_path, num_rows=args.products)
    generate_orders(orders_path, customers_path, products_path, num_rows=args.orders, orphan_rate=args.orphan_rate)
    generate_returns(os.path.join(base, "returns.json"), orders_path, rate=args.return_rate)
    generate_exchange_rates(os.path.join(base, "exchange_rates.csv"))
    generate_inventory_snapshots(os.path.join(base, "inventory_snapshots.csv"), products_path)
    generate_customer_changes(os.path.join(base, "customers_changes.csv"), customers_path)
    print("Generated:")
    print(" -", customers_path)
    print(" -", products_path)
    print(" -", orders_path)


