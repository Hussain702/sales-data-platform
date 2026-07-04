# Data Dictionary — Sales Data Platform

## Overview
Star Schema with 3 dimension tables and 1 fact table, built on Azure Databricks + Delta Lake using the Medallion Architecture (Bronze → Silver → Gold).

---

## GOLD Layer — Star Schema

### `fact_orders`
> Grain: One row per order line item

| Column | Type | Description |
|--------|------|-------------|
| customer_sk | LONG | FK → dim_customer.customer_sk |
| product_sk | LONG | FK → dim_product.product_sk |
| order_date_sk | INT | FK → dim_date.date_key |
| order_id | STRING | Order identifier (degenerate dimension) |
| order_line_id | STRING | Unique line item key (grain) |
| quantity | INT | Units ordered |
| unit_price | DECIMAL(10,2) | Selling price per unit |
| unit_cost | DECIMAL(10,2) | Cost price per unit |
| discount_pct | DECIMAL(5,4) | Discount percentage (0–1) |
| discount_amount | DECIMAL(10,2) | Total discount in currency |
| gross_revenue | DECIMAL(10,2) | quantity × unit_price |
| net_revenue | DECIMAL(10,2) | gross_revenue − discount_amount |
| cogs | DECIMAL(10,2) | Cost of Goods Sold (quantity × unit_cost) |
| gross_profit | DECIMAL(10,2) | net_revenue − cogs |
| gross_profit_pct | DECIMAL(5,2) | gross_profit / net_revenue × 100 |
| order_date | DATE | Partition column |
| source_system | STRING | Originating system |
| batch_id | STRING | ETL batch run ID |
| created_at | TIMESTAMP | Row creation time |
| updated_at | TIMESTAMP | Last update time |

---

### `dim_customer` (SCD Type 2)
> Source: CRM System (`crm_customers_raw` → `crm_customers_clean`)

| Column | Type | Description |
|--------|------|-------------|
| customer_sk | LONG | Surrogate key (hash-based) |
| customer_bk | STRING | Business key from CRM |
| first_name | STRING | First name |
| last_name | STRING | Last name |
| full_name | STRING | Concatenated full name |
| email | STRING | Email address (lowercased) |
| phone | STRING | Phone number (digits only) |
| city | STRING | City |
| state | STRING | State code (uppercased) |
| country | STRING | Country code (uppercased) |
| zip_code | STRING | Postal code |
| customer_segment | STRING | STANDARD / PREMIUM / VIP |
| registration_date | DATE | Account creation date |
| start_date | DATE | Version effective start date |
| end_date | DATE | Version effective end date (NULL = current record) |
| is_current | BOOLEAN | TRUE if current active record |
| created_at | TIMESTAMP | Row creation time |
| updated_at | TIMESTAMP | Last update time |

---

### `dim_product` (SCD Type 2)
> Source: Product System (`products_raw` → `products_clean`)

| Column | Type | Description |
|--------|------|-------------|
| product_sk | LONG | Surrogate key (hash-based) |
| product_bk | STRING | Business key from Product System |
| product_name | STRING | Product name |
| category | STRING | Top-level category |
| sub_category | STRING | Sub-category |
| brand | STRING | Brand name |
| sku | STRING | Stock keeping unit |
| supplier_id | STRING | Supplier reference |
| unit_cost | DECIMAL(10,2) | Cost price |
| unit_price | DECIMAL(10,2) | Retail price |
| gross_margin_pct | DECIMAL(5,2) | (price−cost)/price × 100 |
| is_active | BOOLEAN | Active in catalog |
| start_date | DATE | Version effective start date |
| end_date | DATE | Version effective end date (NULL = current record) |
| is_current | BOOLEAN | TRUE if current active record |
| created_at | TIMESTAMP | Row creation time |
| updated_at | TIMESTAMP | Last update time |

---

### `dim_date` (Static)
> Pre-generated date spine — no SCD tracking required

| Column | Type | Description |
|--------|------|-------------|
| date_key | INT | YYYYMMDD integer key |
| full_date | DATE | Calendar date |
| year | INT | Calendar year |
| quarter | INT | 1–4 |
| quarter_name | STRING | Q1–Q4 |
| month | INT | 1–12 |
| month_name | STRING | January–December |
| month_abbrev | STRING | Jan–Dec |
| week_of_year | INT | ISO week number |
| day_of_month | INT | 1–31 |
| day_of_week | INT | 1=Sun, 7=Sat |
| day_name | STRING | Monday–Sunday |
| is_weekend | BOOLEAN | Saturday or Sunday |
| year_month | STRING | yyyy-MM |
| year_quarter | STRING | yyyy-Q1 |
| fiscal_year | INT | Fiscal year (Jul start) |
| fiscal_quarter | STRING | FQ1–FQ4 |

---

## SILVER Layer

### `silver.crm_customers_clean`
Cleansed CRM customers — type-cast, deduplicated, standardised casing.

| Column | Type | Description |
|--------|------|-------------|
| customer_bk | STRING | Business key from CRM |
| first_name | STRING | First name (trimmed) |
| last_name | STRING | Last name (trimmed) |
| email | STRING | Email (lowercased, validated) |
| phone | STRING | Phone (digits only) |
| city | STRING | City (trimmed) |
| state | STRING | State code (uppercased) |
| country | STRING | Country code (uppercased) |
| zip_code | STRING | Postal code |
| customer_segment | STRING | STANDARD / PREMIUM / VIP |
| registration_date | DATE | Account creation date |
| source_system | STRING | Originating system |
| batch_id | STRING | ETL batch run ID |
| ingested_at | TIMESTAMP | Bronze ingestion time |
| updated_at | TIMESTAMP | Silver processing time |

---

### `silver.products_clean`
Cleansed product data — numeric types enforced, invalid pricing removed, `gross_margin_pct` derived.

| Column | Type | Description |
|--------|------|-------------|
| product_bk | STRING | Business key from Product System |
| product_name | STRING | Product name (trimmed) |
| category | STRING | Top-level category |
| sub_category | STRING | Sub-category |
| brand | STRING | Brand name |
| sku | STRING | Stock keeping unit |
| supplier_id | STRING | Supplier reference |
| unit_cost | DECIMAL(10,2) | Cost price (validated > 0) |
| unit_price | DECIMAL(10,2) | Retail price (validated > unit_cost) |
| gross_margin_pct | DECIMAL(5,2) | (price−cost)/price × 100 (derived) |
| is_active | BOOLEAN | Active in catalog |
| source_system | STRING | Originating system |
| batch_id | STRING | ETL batch run ID |
| ingested_at | TIMESTAMP | Bronze ingestion time |
| updated_at | TIMESTAMP | Silver processing time |

---

### `silver.orders_clean`
Cleansed orders — composite key deduplication, financial metrics derived.

| Column | Type | Description |
|--------|------|-------------|
| order_id | STRING | Order identifier |
| order_line_id | STRING | Unique line item key |
| customer_bk | STRING | Customer business key (FK to CRM) |
| product_bk | STRING | Product business key (FK to ERP) |
| order_date | DATE | Order date |
| quantity | INT | Units ordered (validated > 0) |
| unit_price | DECIMAL(10,2) | Selling price per unit |
| unit_cost | DECIMAL(10,2) | Cost price per unit |
| discount_pct | DECIMAL(5,4) | Discount percentage (0–1) |
| discount_amount | DECIMAL(10,2) | quantity × unit_price × discount_pct (derived) |
| gross_revenue | DECIMAL(10,2) | quantity × unit_price (derived) |
| net_revenue | DECIMAL(10,2) | gross_revenue − discount_amount (derived) |
| cogs | DECIMAL(10,2) | quantity × unit_cost (derived) |
| source_system | STRING | Originating system |
| batch_id | STRING | ETL batch run ID |
| ingested_at | TIMESTAMP | Bronze ingestion time |
| updated_at | TIMESTAMP | Silver processing time |

---

## BRONZE Layer

### `bronze.crm_customers_raw`
Raw CRM data — all columns ingested as strings, no transformation applied.

| Column | Type | Description |
|--------|------|-------------|
| * (all source columns) | STRING | Raw source values, cast to string |
| source_system | STRING | Hardcoded source identifier |
| batch_id | STRING | ETL batch run ID |
| ingested_at | TIMESTAMP | Ingestion timestamp |

---

### `bronze.products_raw`
Raw product data — all columns ingested as strings.

| Column | Type | Description |
|--------|------|-------------|
| * (all source columns) | STRING | Raw source values, cast to string |
| source_system | STRING | Hardcoded source identifier |
| batch_id | STRING | ETL batch run ID |
| ingested_at | TIMESTAMP | Ingestion timestamp |

---

### `bronze.orders_raw`
Raw order data — all columns ingested as strings. Partitioned by `order_date`.

| Column | Type | Description |
|--------|------|-------------|
| * (all source columns) | STRING | Raw source values, cast to string |
| source_system | STRING | Hardcoded source identifier |
| batch_id | STRING | ETL batch run ID |
| ingested_at | TIMESTAMP | Ingestion timestamp |
| order_date | DATE | Partition column |

---

## SCD Type 2 — Behaviour Summary

| Event | Action |
|-------|--------|
| New record arrives | INSERT with new surrogate key; `start_date` = today, `end_date` = NULL, `is_current` = TRUE |
| Existing record unchanged | No action |
| Existing record changed | Expire old row (`end_date` = today − 1, `is_current` = FALSE); INSERT new version (`start_date` = today, `is_current` = TRUE) |
| Record deleted from source | No action — history retained |