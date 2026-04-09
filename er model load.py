

import sys
import pandas as pd
import mysql.connector
from mysql.connector import Error

DB_CONFIG = {
    "host":     "localhost",
    "port":     3306,
    "user":     "root",
    "password": "root",
    "database": "projekt",
    "charset":  "utf8mb4",
}

CSV_PATH = r"C:\Users\User\Desktop\projekt.csv"


def get_connection():
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        print("  Connected to MySQL.")
        return conn
    except Error as e:
        print(f"  ERROR: Cannot connect to MySQL — {e}")
        sys.exit(1)


def bulk_insert(cursor, sql, rows, label):
    try:
        cursor.executemany(sql, rows)
        print(f"  {label:<20} {cursor.rowcount:>6} rows inserted  ({len(rows)} unique)")
    except Error as e:
        print(f"  ERROR inserting {label}: {e}")
        raise


def load_csv(path: str) -> pd.DataFrame:
    print(f"\n[1/9] Loading CSV from: {path}")
    try:
        df = pd.read_csv(path, encoding="latin-1")
    except FileNotFoundError:
        print(f"  ERROR: File not found — {path}")
        sys.exit(1)

    df.columns = (
        df.columns
        .str.strip()
        .str.lower()
        .str.replace(" ", "_", regex=False)
        .str.replace("-", "_", regex=False)
    )

    print(f"  Shape: {len(df):,} rows x {len(df.columns)} columns")
    print(f"  Columns found in CSV:")
    for col in df.columns:
        print(f"    '{col}'")

   
    COLUMN_MAP = {
        "row_id":        ["row_id", "row id"],
        "order_id":      ["order_id", "order id"],
        "order_date":    ["order_date", "order date"],
        "ship_date":     ["ship_date", "ship date"],
        "ship_mode":     ["ship_mode", "ship mode"],
        "customer_id":   ["customer_id", "customer id"],
        "customer_name": ["customer_name", "customer name"],
        "segment":       ["segment"],
        "city":          ["city"],
        "state":         ["state"],
        "country":       ["country"],
        "postal_code":   ["postal_code", "postal code"],
        "region":        ["region"],
        "market":        ["market"],
        "product_id":    ["product_id", "product id"],
        "category":      ["category"],
        "sub_category":  ["sub_category", "sub-category", "subcategory"],
        "product_name":  ["product_name", "product name"],
        "sales":         ["sales"],
        "quantity":      ["quantity"],
        "discount":      ["discount"],
        "profit":        ["profit"],
        "shipping_cost": ["shipping_cost", "shipping cost", "freight", "shipping"],
        "order_priority":["order_priority", "order priority"],
    }

    rename = {}
    missing = []
    for target, variants in COLUMN_MAP.items():
        matched = next((v for v in variants if v in df.columns), None)
        if matched and matched != target:
            rename[matched] = target
        elif not matched:
            missing.append(target)

    if rename:
        df = df.rename(columns=rename)
        print(f"\n  Renamed columns: {rename}")

    if missing:
        print(f"\n  WARNING: These columns were not found and will default to 0 or N/A:")
        for m in missing:
            print(f"    '{m}'")
            df[m] = "N/A" if m in ("postal_code", "market", "region") else 0

    df["order_date"] = pd.to_datetime(df["order_date"], dayfirst=False, errors="coerce")
    df["ship_date"]  = pd.to_datetime(df["ship_date"],  dayfirst=False, errors="coerce")

    bad_dates = df["order_date"].isna().sum() + df["ship_date"].isna().sum()
    if bad_dates > 0:
        print(f"  WARNING: {bad_dates} rows with invalid dates will be dropped")
        df = df.dropna(subset=["order_date", "ship_date"])

    df["postal_code"]   = df["postal_code"].fillna("N/A").astype(str)
    df["sales"]         = pd.to_numeric(df["sales"],         errors="coerce").fillna(0)
    df["profit"]        = pd.to_numeric(df["profit"],        errors="coerce").fillna(0)
    df["discount"]      = pd.to_numeric(df["discount"],      errors="coerce").fillna(0)
    df["shipping_cost"] = pd.to_numeric(df["shipping_cost"], errors="coerce").fillna(0)
    df["quantity"]      = pd.to_numeric(df["quantity"],      errors="coerce").fillna(1).astype(int)

   
    df["supplier_name"] = df["product_name"].str.split().str[0].str.strip()

    print(f"\n  Date range: {df['order_date'].min().date()} -> {df['order_date'].max().date()}")
    print(f"  Ready: {len(df):,} clean rows\n")
    return df


def insert_customers(cursor, df: pd.DataFrame):
    customers = (
        df[["customer_id", "customer_name", "segment"]]
        .drop_duplicates(subset=["customer_id"])
        .sort_values("customer_id")
    )
    rows = [
        (r.customer_id, r.customer_name, r.segment)
        for r in customers.itertuples(index=False)
    ]
    bulk_insert(cursor,
        "INSERT IGNORE INTO Customers (CustomerID, CustomerName, Segment) VALUES (%s, %s, %s)",
        rows, "Customers"
    )



def insert_locations(cursor, df: pd.DataFrame):
    locations = (
        df[["city", "state", "country", "postal_code", "region", "market"]]
        .drop_duplicates()
        .sort_values(["country", "city"])
    )
    rows = [
        (r.city, r.state, r.country, r.postal_code, r.region, r.market)
        for r in locations.itertuples(index=False)
    ]
    bulk_insert(cursor,
        """INSERT IGNORE INTO Locations (City, State, Country, PostalCode, Region, Market)
           VALUES (%s, %s, %s, %s, %s, %s)""",
        rows, "Locations"
    )


def insert_shippers(cursor, df: pd.DataFrame):
    shippers = df[["ship_mode"]].drop_duplicates().sort_values("ship_mode")
    rows = [(r.ship_mode,) for r in shippers.itertuples(index=False)]
    bulk_insert(cursor,
        "INSERT IGNORE INTO Shippers (ShipMode) VALUES (%s)",
        rows, "Shippers"
    )


def insert_suppliers(cursor, df: pd.DataFrame):
    suppliers = (
        df[["supplier_name"]]
        .drop_duplicates()
        .sort_values("supplier_name")
    )
    rows = [(r.supplier_name,) for r in suppliers.itertuples(index=False)]
    bulk_insert(cursor,
        "INSERT IGNORE INTO Suppliers (SupplierName) VALUES (%s)",
        rows, "Suppliers"
    )


def insert_categories(cursor, df: pd.DataFrame):
    categories = (
        df[["category", "sub_category"]]
        .drop_duplicates()
        .sort_values(["category", "sub_category"])
    )
    rows = [(r.category, r.sub_category) for r in categories.itertuples(index=False)]
    bulk_insert(cursor,
        "INSERT IGNORE INTO Categories (CategoryName, SubCategory) VALUES (%s, %s)",
        rows, "Categories"
    )


def insert_products(cursor, df: pd.DataFrame):
    cursor.execute("SELECT CategoryID, CategoryName, SubCategory FROM Categories")
    cat_map = {(row[1], row[2]): row[0] for row in cursor.fetchall()}

    cursor.execute("SELECT SupplierID, SupplierName FROM Suppliers")
    sup_map = {row[1]: row[0] for row in cursor.fetchall()}

    products = (
        df[["product_id", "product_name", "category", "sub_category", "supplier_name"]]
        .drop_duplicates(subset=["product_id"])
        .sort_values("product_id")
    )

    rows = []
    unresolved_cats = set()
    for r in products.itertuples(index=False):
        cat_id = cat_map.get((r.category, r.sub_category))
        sup_id = sup_map.get(r.supplier_name)
        if cat_id is None:
            unresolved_cats.add((r.category, r.sub_category))
            continue
        rows.append((r.product_id, r.product_name, cat_id, sup_id))

    if unresolved_cats:
        print(f"  WARNING: {len(unresolved_cats)} unresolved category pairs skipped")

    bulk_insert(cursor,
        """INSERT IGNORE INTO Products (ProductID, ProductName, CategoryID, SupplierID)
           VALUES (%s, %s, %s, %s)""",
        rows, "Products"
    )


def insert_orders(cursor, df: pd.DataFrame):
    cursor.execute("SELECT LocationID, City, State, Country, Market FROM Locations")
    loc_map = {(row[1], row[2], row[3], row[4]): row[0] for row in cursor.fetchall()}

    cursor.execute("SELECT ShipperID, ShipMode FROM Shippers")
    ship_map = {row[1]: row[0] for row in cursor.fetchall()}

    orders = (
        df[["order_id", "customer_id", "city", "state", "country", "market",
            "ship_mode", "order_date", "ship_date", "order_priority"]]
        .drop_duplicates(subset=["order_id"])
        .sort_values("order_date")
    )

    rows = []
    skipped = 0
    for r in orders.itertuples(index=False):
        loc_id  = loc_map.get((r.city, r.state, r.country, r.market))
        ship_id = ship_map.get(r.ship_mode)
        if loc_id is None or ship_id is None:
            skipped += 1
            continue
        rows.append((
            r.order_id,
            r.customer_id,
            loc_id,
            ship_id,
            r.order_date.date(),
            r.ship_date.date(),
            r.order_priority
        ))

    if skipped:
        print(f"  WARNING: {skipped} orders skipped (could not resolve location or shipper)")

    bulk_insert(cursor,
        """INSERT IGNORE INTO Orders
           (OrderID, CustomerID, LocationID, ShipperID, OrderDate, ShipDate, OrderPriority)
           VALUES (%s, %s, %s, %s, %s, %s, %s)""",
        rows, "Orders"
    )



def insert_order_lines(cursor, conn, df: pd.DataFrame):
    print(f"\n[9/9] Inserting Order_Lines ({len(df):,} rows)...")

    rows = []
    for r in df.itertuples(index=False):
        rows.append((
            r.order_id,
            r.product_id,
            round(float(r.sales),        2),
            int(r.quantity),
            round(float(r.discount),     4),
            round(float(r.profit),       2),
            round(float(r.shipping_cost),2),
        ))

    sql = """INSERT INTO Order_Lines
             (OrderID, ProductID, Sales, Quantity, Discount, Profit, ShippingCost)
             VALUES (%s, %s, %s, %s, %s, %s, %s)"""

    batch_size     = 2000
    total_inserted = 0

    for i in range(0, len(rows), batch_size):
        batch = rows[i : i + batch_size]
        try:
            cursor.executemany(sql, batch)
            conn.commit()
            total_inserted += len(batch)
            pct = total_inserted / len(rows) * 100
            print(f"  Progress: {total_inserted:>6,} / {len(rows):,}  ({pct:.1f}%)", end="\r")
        except Error as e:
            print(f"\n  ERROR on batch {i}-{i+batch_size}: {e}")
            raise

    print(f"\n  Order_Lines        {total_inserted:>6,} rows inserted")





def main():
    print("=" * 55)
    print("  Global Superstore - CSV to Relational Model (OLTP)")
    print("=" * 55)

    df   = load_csv(CSV_PATH)
    conn = get_connection()
    cursor = conn.cursor()

    try:
        print("\n--- Inserting lookup tables ---")
        insert_customers(cursor, df)
        insert_locations(cursor, df)
        insert_shippers(cursor, df)
        insert_suppliers(cursor, df)
        insert_categories(cursor, df)
        conn.commit()
        print("  Lookup tables committed.")

        print("\n--- Inserting tables with FK dependencies ---")
        insert_products(cursor, df)
        conn.commit()
        print("  Products committed.")

        insert_orders(cursor, df)
        conn.commit()
        print("  Orders committed.")

        cursor.close()

        cursor = conn.cursor()
        insert_order_lines(cursor, conn, df)
        cursor.close()

        print("\n  Done. All OLTP tables populated.")

    except Exception as e:
        conn.rollback()
        print(f"\n  ETL failed and rolled back: {e}")
        raise

    finally:
        conn.close()
        print("  Connection closed.")


if __name__ == "__main__":
    main()