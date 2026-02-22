"""
Generate a realistic sample CSV that mimics the Online Retail II dataset.
This script creates 500K rows of e-commerce transaction data.

Run this if you don't want to download from Kaggle/UCI:
    python data/sample/generate_sample.py

Output: data/raw/online_retail_II.csv
"""

import pandas as pd
import numpy as np
import random
import os
from datetime import datetime, timedelta

random.seed(42)
np.random.seed(42)

OUTPUT_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "raw",
    "online_retail_II.csv"
)

N_RECORDS = 520000

PRODUCTS = [
    ("85123A", "WHITE HANGING HEART T-LIGHT HOLDER", 2.55),
    ("71053",  "WHITE METAL LANTERN",                3.39),
    ("84406B", "CREAM CUPID HEARTS COAT HANGER",     2.75),
    ("84029G", "KNITTED UNION FLAG HOT WATER BOTTLE", 3.39),
    ("84029E", "RED WOOLLY HOTTIE WHITE HEART",       3.39),
    ("22752",  "SET 7 BABUSHKA NESTING BOXES",        7.65),
    ("21730",  "GLASS STAR FROSTED T-LIGHT HOLDER",   4.25),
    ("22633",  "HAND WARMER UNION JACK",              1.85),
    ("22632",  "HAND WARMER RED POLKA DOT",           1.85),
    ("84879",  "ASSORTED COLOUR BIRD ORNAMENT",       1.69),
    ("47566",  "PARTY BUNTING",                       4.95),
    ("85099B", "JUMBO BAG RED RETROSPOT",             1.95),
    ("21212",  "PACK OF 72 RETROSPOT CAKE CASES",     0.42),
    ("22423",  "REGENCY CAKESTAND 3 TIER",           10.95),
    ("47566B", "PARTY BUNTING LARGE",                 6.95),
    ("20725",  "LUNCH BAG RED RETROSPOT",             1.65),
    ("20727",  "LUNCH BAG BLACK SKULL",               1.65),
    ("22960",  "JAM MAKING SET WITH JARS",            4.25),
    ("21977",  "PACK OF 60 PINK PAISLEY CAKE CASES",  0.55),
    ("21980",  "PACK OF 12 LONDON TISSUES",           0.29),
    ("22111",  "SCOTTIE DOG HOT WATER BOTTLE",        4.95),
    ("23084",  "RABBIT NIGHT LIGHT",                  1.69),
    ("22469",  "HEART OF WICKER SMALL",               1.65),
    ("85123",  "PINK HANGING HEART T-LIGHT HOLDER",   2.55),
    ("22720",  "SET OF 3 CAKE TINS PANTRY DESIGN",    4.95),
    ("22722",  "SET OF 6 SPICE TINS PANTRY DESIGN",   4.25),
    ("22357",  "KINGS CHOICE BISCUIT TIN",            3.75),
    ("22551",  "PLASTERS IN TIN WOODLAND ANIMALS",    1.25),
    ("21931",  "JUMBO STORAGE BAG SUKI",              1.95),
    ("22386",  "JUMBO BAG PINK POLKADOT",             1.95),
]

COUNTRIES = [
    ("United Kingdom", 0.52),
    ("Germany",        0.08),
    ("France",         0.07),
    ("EIRE",           0.05),
    ("Spain",          0.04),
    ("Netherlands",    0.04),
    ("Belgium",        0.03),
    ("Switzerland",    0.02),
    ("Portugal",       0.02),
    ("Australia",      0.02),
    ("Norway",         0.01),
    ("Italy",          0.01),
    ("Sweden",         0.01),
    ("Denmark",        0.01),
    ("Finland",        0.01),
    ("Japan",          0.01),
    ("USA",            0.01),
    ("Canada",         0.005),
    ("Brazil",         0.005),
    ("Singapore",      0.005),
    ("Unspecified",    0.005),
]

country_names  = [c[0] for c in COUNTRIES]
country_weights = [c[1] for c in COUNTRIES]

CUSTOMER_IDS = [str(random.randint(12000, 18500)) for _ in range(4500)]
CUSTOMER_IDS += [None] * 1200

start_date = datetime(2009, 12, 1)
end_date   = datetime(2011, 12, 9)
date_range_secs = int((end_date - start_date).total_seconds())


def random_date():
    offset = random.randint(0, date_range_secs)
    dt = start_date + timedelta(seconds=offset)
    return f"{dt.month}/{dt.day}/{dt.year} {dt.hour:02d}:{dt.minute:02d}"


def random_invoice():
    is_cancellation = random.random() < 0.02
    prefix = "C" if is_cancellation else ""
    return prefix + str(random.randint(489000, 581000))


print(f"Generating {N_RECORDS:,} records...")

rows = []
for i in range(N_RECORDS):
    product = random.choice(PRODUCTS)
    qty_raw = random.choices(
        [random.randint(1, 12), random.randint(13, 100), random.randint(-12, -1)],
        weights=[0.75, 0.15, 0.10]
    )[0]

    price_jitter = round(product[2] * random.uniform(0.9, 1.15), 2)

    row = {
        "Invoice":     random_invoice(),
        "StockCode":   product[0],
        "Description": product[1] if random.random() > 0.01 else None,
        "Quantity":    qty_raw,
        "InvoiceDate": random_date(),
        "Price":       price_jitter if random.random() > 0.005 else None,
        "Customer ID": random.choice(CUSTOMER_IDS),
        "Country":     random.choices(country_names, weights=country_weights)[0],
    }
    rows.append(row)

    if (i + 1) % 50000 == 0:
        print(f"  {i + 1:,} / {N_RECORDS:,} rows generated...")

df = pd.DataFrame(rows, columns=[
    "Invoice", "StockCode", "Description", "Quantity",
    "InvoiceDate", "Price", "Customer ID", "Country"
])

# Inject ~3% duplicates to simulate real-world data issues
n_dups = int(N_RECORDS * 0.03)
dup_rows = df.sample(n=n_dups, random_state=42)
df = pd.concat([df, dup_rows], ignore_index=True).sample(frac=1, random_state=42).reset_index(drop=True)

os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)
df.to_csv(OUTPUT_PATH, index=False)

print(f"\nDone. File saved to: {OUTPUT_PATH}")
print(f"Total rows (with injected duplicates): {len(df):,}")
print(f"Null Customer IDs : {df['Customer ID'].isna().sum():,}")
print(f"Null Prices       : {df['Price'].isna().sum():,}")
print(f"Null Descriptions : {df['Description'].isna().sum():,}")
print(f"Sample rows:")
print(df.head(3).to_string())
