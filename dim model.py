

import sys
from datetime import date, timedelta
from collections import defaultdict
import mysql.connector
from mysql.connector import Error

DB_CONFIG = {
    "host":     "localhost",
    "port":     3306,
    "user":     "root",
    "password": "root",
    "database": "dim_model_projekt",
    "charset":  "utf8mb4",
}

SOURCE_DB = "projekt"
TARGET_DB = "dim_model_projekt"


def get_connection():
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        print("  Connected to MySQL.")
        return conn
    except Error as e:
        print(f"  ERROR: Cannot connect — {e}")
        sys.exit(1)


def bulk_insert(cursor, sql, rows, label):
    try:
        cursor.executemany(sql, rows)
        print(f"  {label:<20} {cursor.rowcount:>6,} rows inserted  ({len(rows):,} prepared)")
    except Error as e:
        print(f"  ERROR inserting {label}: {e}")
        raise


def fetch_all(cursor, sql):
    cursor.execute(sql)
    return cursor.fetchall()



def insert_dim_date(cursor, conn):
    print("\n[1/6] Dim_Date - generating date range from Orders...")

    cursor.execute(f"SELECT MIN(OrderDate), MAX(ShipDate) FROM {SOURCE_DB}.Orders")
    row = cursor.fetchone()
    if not row or row[0] is None:
        print("  ERROR: No orders found. Populate the OLTP database first.")
        sys.exit(1)

    start_date = row[0]
    end_date   = row[1]
    print(f"  Range: {start_date} -> {end_date}")

    rows = []
    d = start_date
    while d <= end_date:
        iso     = d.isocalendar()
        dow     = d.isoweekday()       
        quarter = (d.month - 1) // 3 + 1
        weekend = 1 if dow >= 6 else 0

        rows.append((
            int(d.strftime("%Y%m%d")), 
            d,                         
            d.year,
            quarter,
            f"Q{quarter}",
            d.month,
            d.strftime("%B"),          
            d.strftime("%b"),          
            iso[1],                    
            d.day,
            dow,
            d.strftime("%A"),          
            weekend,
        ))
        d += timedelta(days=1)

    bulk_insert(cursor,
        """INSERT IGNORE INTO Dim_Date
           (DateKey, FullDate, Year, Quarter, QuarterName,
            Month, MonthName, MonthShort, Week,
            DayOfMonth, DayOfWeek, DayName, IsWeekend)
           VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
        rows, "Dim_Date"
    )
    conn.commit()



def insert_dim_product(cursor, conn):
    print("\n[2/6] Dim_Product - denormalizing Products + Categories + Suppliers...")

    rows_raw = fetch_all(cursor, f"""
        SELECT
            p.ProductID,
            p.ProductName,
            c.SubCategory,
            c.CategoryName,
            COALESCE(s.SupplierName, 'Unknown') AS SupplierName
        FROM {SOURCE_DB}.Products p
        JOIN  {SOURCE_DB}.Categories c ON p.CategoryID = c.CategoryID
        LEFT JOIN {SOURCE_DB}.Suppliers s ON p.SupplierID = s.SupplierID
        ORDER BY c.CategoryName, c.SubCategory, p.ProductName
    """)

    rows = [(r[0], r[1], r[2], r[3], r[4]) for r in rows_raw]

    bulk_insert(cursor,
        """INSERT IGNORE INTO Dim_Product
           (ProductID, ProductName, SubCategory, Category, SupplierName)
           VALUES (%s, %s, %s, %s, %s)""",
        rows, "Dim_Product"
    )
    conn.commit()



def insert_dim_customer(cursor, conn):
    print("\n[3/6] Dim_Customer - building SCD Type 2 history...")

    rows_raw = fetch_all(cursor, f"""
        SELECT
            c.CustomerID,
            c.CustomerName,
            c.Segment,
            MIN(o.OrderDate) AS FirstSeen
        FROM {SOURCE_DB}.Customers c
        JOIN {SOURCE_DB}.Orders o ON c.CustomerID = o.CustomerID
        GROUP BY c.CustomerID, c.CustomerName, c.Segment
        ORDER BY c.CustomerID, FirstSeen
    """)

    customer_versions = defaultdict(list)
    for cid, cname, seg, first_seen in rows_raw:
        customer_versions[cid].append((cname, seg, first_seen))

    rows = []
    for cid, versions in customer_versions.items():
        for i, (cname, seg, valid_from) in enumerate(versions):
            is_last = (i == len(versions) - 1)
            if is_last:
                valid_to   = None
                is_current = 1
            else:
                valid_to   = versions[i + 1][2] - timedelta(days=1)
                is_current = 0

            rows.append((cid, cname, seg, valid_from, valid_to, is_current))

    bulk_insert(cursor,
        """INSERT IGNORE INTO Dim_Customer
           (CustomerID, CustomerName, Segment, ValidFrom, ValidTo, IsCurrent)
           VALUES (%s, %s, %s, %s, %s, %s)""",
        rows, "Dim_Customer"
    )
    conn.commit()
    print(f"  SCD2 versions built: {len(rows)} rows for {len(customer_versions)} customers")


def insert_dim_location(cursor, conn):
    print("\n[4/6] Dim_Location - loading geographic data...")

    rows_raw = fetch_all(cursor, f"""
        SELECT DISTINCT
            City, State, Country, Market, Region,
            COALESCE(PostalCode, 'N/A') AS PostalCode
        FROM {SOURCE_DB}.Locations
        ORDER BY Market, Country, State, City
    """)

    rows = [(r[0], r[1], r[2], r[3], r[4], r[5]) for r in rows_raw]

    bulk_insert(cursor,
        """INSERT IGNORE INTO Dim_Location
           (City, State, Country, Market, Region, PostalCode)
           VALUES (%s, %s, %s, %s, %s, %s)""",
        rows, "Dim_Location"
    )
    conn.commit()



def insert_dim_shipmode(cursor, conn):
    print("\n[5/6] Dim_ShipMode - loading ship mode and priority combinations...")

    rows_raw = fetch_all(cursor, f"""
        SELECT DISTINCT
            s.ShipMode,
            o.OrderPriority
        FROM {SOURCE_DB}.Orders o
        JOIN {SOURCE_DB}.Shippers s ON o.ShipperID = s.ShipperID
        ORDER BY s.ShipMode, o.OrderPriority
    """)

    rows = [(r[0], r[1]) for r in rows_raw]

    bulk_insert(cursor,
        "INSERT IGNORE INTO Dim_ShipMode (ShipMode, OrderPriority) VALUES (%s, %s)",
        rows, "Dim_ShipMode"
    )
    conn.commit()



def insert_fact_sales(cursor, conn):
    print("\n[6/6] Fact_Sales - resolving surrogate keys and inserting...")

    print("  Loading dimension key maps...")

    cursor.execute("SELECT ProductKey, ProductID FROM Dim_Product")
    product_map = {row[1]: row[0] for row in cursor.fetchall()}
    print(f"    Dim_Product:  {len(product_map):,} keys")

    cursor.execute("""
        SELECT CustomerKey, CustomerID, ValidFrom, ValidTo, IsCurrent
        FROM Dim_Customer
        ORDER BY CustomerID, ValidFrom
    """)
    customer_rows = cursor.fetchall()
    print(f"    Dim_Customer: {len(customer_rows):,} SCD2 records")

    cursor.execute("SELECT LocationKey, City, State, Country, Market FROM Dim_Location")
    location_map = {(r[1], r[2], r[3], r[4]): r[0] for r in cursor.fetchall()}
    print(f"    Dim_Location: {len(location_map):,} keys")

    cursor.execute("SELECT ShipModeKey, ShipMode, OrderPriority FROM Dim_ShipMode")
    shipmode_map = {(r[1], r[2]): r[0] for r in cursor.fetchall()}
    print(f"    Dim_ShipMode: {len(shipmode_map):,} keys")

    cust_index = defaultdict(list)
    for ckey, cid, vfrom, vto, is_curr in customer_rows:
        cust_index[cid].append((ckey, vfrom, vto))

  
    def get_customer_key(cust_id, order_date):
        if isinstance(order_date, str):
            order_date = date.fromisoformat(order_date)
        for ckey, vfrom, vto in cust_index.get(cust_id, []):
            end = vto if vto else date(9999, 12, 31)
            if vfrom <= order_date <= end:
                return ckey
        return None

    print("  Fetching order lines from OLTP...")
    cursor.execute(f"""
        SELECT
            ol.OrderID,
            ol.ProductID,
            o.CustomerID,
            loc.City,
            loc.State,
            loc.Country,
            loc.Market,
            s.ShipMode,
            o.OrderPriority,
            o.OrderDate,
            o.ShipDate,
            ol.Sales,
            ol.Quantity,
            ol.Discount,
            ol.Profit,
            ol.ShippingCost
        FROM {SOURCE_DB}.Order_Lines ol
        JOIN {SOURCE_DB}.Orders    o   ON ol.OrderID   = o.OrderID
        JOIN {SOURCE_DB}.Locations loc ON o.LocationID = loc.LocationID
        JOIN {SOURCE_DB}.Shippers  s   ON o.ShipperID  = s.ShipperID
        ORDER BY o.OrderDate
    """)
    source_rows = cursor.fetchall()
    print(f"  Source rows fetched: {len(source_rows):,}")

    print("  Resolving surrogate keys...")
    fact_rows = []
    skipped   = 0
    skip_log  = {}

    for row in source_rows:
        (order_id, product_id, customer_id,
         city, state, country, market,
         ship_mode, order_priority,
         order_date, ship_date,
         sales, qty, discount, profit, shipping_cost) = row

        prod_key = product_map.get(product_id)
        cust_key = get_customer_key(customer_id, order_date)
        loc_key  = location_map.get((city, state, country, market))
        ship_key = shipmode_map.get((ship_mode, order_priority))
        order_dk = int(order_date.strftime("%Y%m%d"))
        ship_dk  = int(ship_date.strftime("%Y%m%d"))
        del_days = (ship_date - order_date).days

        if None in (prod_key, cust_key, loc_key, ship_key):
            reason = []
            if prod_key is None: reason.append(f"ProductID={product_id}")
            if cust_key is None: reason.append(f"CustomerID={customer_id}")
            if loc_key  is None: reason.append(f"Loc=({city},{country},{market})")
            if ship_key is None: reason.append(f"Ship=({ship_mode},{order_priority})")
            key = ", ".join(reason)
            skip_log[key] = skip_log.get(key, 0) + 1
            skipped += 1
            continue

        fact_rows.append((
            order_id,                        
            prod_key,
            cust_key,
            loc_key,
            ship_key,
            order_dk,                        
            ship_dk,                         
            round(float(sales),        2),
            int(qty),
            round(float(discount),     4),
            round(float(profit),       2),
            round(float(shipping_cost),2),
            max(del_days, 0),                
        ))

    if skipped > 0:
        print(f"\n  WARNING: {skipped} rows skipped due to unresolved surrogate keys:")
        for reason, count in list(skip_log.items())[:10]:
            print(f"    {count:>4}x  {reason}")

    print(f"\n  Inserting {len(fact_rows):,} fact rows in batches of 2,000...")
    sql = """
        INSERT INTO Fact_Sales
        (OrderID, ProductKey, CustomerKey, LocationKey, ShipModeKey,
         OrderDateKey, ShipDateKey,
         Sales, Quantity, Discount, Profit, ShippingCost, DeliveryDays)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    """

    batch_size     = 2000
    total_inserted = 0

    for i in range(0, len(fact_rows), batch_size):
        batch = fact_rows[i : i + batch_size]
        try:
            cursor.executemany(sql, batch)
            conn.commit()
            total_inserted += len(batch)
            pct = total_inserted / len(fact_rows) * 100
            print(f"  Progress: {total_inserted:>6,} / {len(fact_rows):,}  ({pct:.1f}%)", end="\r")
        except Error as e:
            print(f"\n  ERROR on batch starting at row {i}: {e}")
            raise

    print(f"\n  Fact_Sales         {total_inserted:>6,} rows inserted")





def main():
    print("=" * 55)
    print("  Global Superstore - Populate Dimensional Model")
    print("=" * 55)

    conn   = get_connection()
    cursor = conn.cursor()

    try:
        insert_dim_date(cursor, conn)
        insert_dim_product(cursor, conn)
        insert_dim_customer(cursor, conn)
        insert_dim_location(cursor, conn)
        insert_dim_shipmode(cursor, conn)
        insert_fact_sales(cursor, conn)

        print("\n  Done. Dimensional model fully populated.")

    except Exception as e:
        conn.rollback()
        print(f"\n  Failed and rolled back: {e}")
        raise

    finally:
        cursor.close()
        conn.close()
        print("  Connection closed.")


if __name__ == "__main__":
    main()