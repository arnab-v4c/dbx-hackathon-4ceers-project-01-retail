# DBX Retail — Bronze Layer Data Profile

> **Catalog:** `dbx_retail` | **Schema:** `bronze` | **Platform:** Databricks (Unity Catalog) | **Profiled:** 2026-03-23

This document provides a complete exploratory data analysis (EDA) of the 16 bronze-layer tables in the `dbx_retail` data lakehouse. It is designed to give an LLM or data engineer full context for downstream silver/gold layer transformations, data quality remediation, and analytics development.

---

## 1. Dataset Overview

The bronze layer contains **16 tables** organized into **6 entity groups**, totaling **~16,151 rows**. All data originates from CSV and JSON files ingested from `/Volumes/dbx_retail/bronze/raw`.

| Entity Group | Tables | Combined Rows | Purpose |
|---|---|---|---|
| Customers | customers_1 through customers_6 | 748 | Customer master data (demographics, geography, segment) |
| Orders | orders_1 through orders_3 | 5,006 | Order headers (status, shipping, dates, vendor linkage) |
| Products | products | 1,774 | Product catalog (brand, categories, sizes, UPC) |
| Returns | returns_1, returns_2 | 800 | Return/refund records (reason, status, amount) |
| Transactions | transactions_1 through transactions_3 | 9,816 | Order line items (sales, quantity, discount, profit, payment) |
| Vendors | vendors | 7 | Vendor reference (ID + name) |

---

## 2. Table-Level Summary

| Table | FQN | Rows | Columns | Column Names |
|---|---|---|---|---|
| customers_1 | `dbx_retail.bronze.customers_1` | 87 | 9 | customer_id, customer_email, customer_name, segment, country, city, state, postal_code, region |
| customers_2 | `dbx_retail.bronze.customers_2` | 112 | 9 | CustomerID, customer_email, customer_name, segment, country, city, state, postal_code, region |
| customers_3 | `dbx_retail.bronze.customers_3` | 53 | 9 | cust_id, customer_email, customer_name, segment, country, city, state, postal_code, region |
| customers_4 | `dbx_retail.bronze.customers_4` | 80 | 9 | customer_id, customer_email, customer_name, segment, country, state, city, postal_code, region |
| customers_5 | `dbx_retail.bronze.customers_5` | 42 | 8 | customer_id, customer_name, segment, country, city, state, postal_code, region |
| customers_6 | `dbx_retail.bronze.customers_6` | 374 | 9 | customer_identifier, email_address, full_name, customer_segment, country, city, state, postal_code, region |
| orders_1 | `dbx_retail.bronze.orders_1` | 2,002 | 10 | order_id, customer_id, vendor_id, ship_mode, order_status, order_purchase_date, order_approved_at, order_delivered_carrier_date, order_delivered_customer_date, order_estimated_delivery_date |
| orders_2 | `dbx_retail.bronze.orders_2` | 1,752 | 10 | order_id, customer_id, vendor_id, ship_mode, order_status, order_purchase_date, order_approved_at, order_delivered_carrier_date, order_delivered_customer_date, order_estimated_delivery_date |
| orders_3 | `dbx_retail.bronze.orders_3` | 1,252 | 9 | order_id, customer_id, vendor_id, ship_mode, order_status, order_purchase_date, order_approved_at, order_delivered_carrier_date, order_delivered_customer_date |
| products | `dbx_retail.bronze.products` | 1,774 | 13 | brand, categories, colors, dateAdded, dateUpdated, dimension, manufacturer, product_id, product_name, product_photos_qty, sizes, upc, weight |
| returns_1 | `dbx_retail.bronze.returns_1` | 500 | 5 | order_id, refund_amount, return_date, return_reason, return_status |
| returns_2 | `dbx_retail.bronze.returns_2` | 300 | 5 | OrderId, amount, date_of_return, reason, status |
| transactions_1 | `dbx_retail.bronze.transactions_1` | 3,730 | 8 | Order_id, Product_id, Sales, Quantity, discount, profit, payment_type, payment_installments |
| transactions_2 | `dbx_retail.bronze.transactions_2` | 3,337 | 7 | Order_id, Product_id, Sales, Quantity, discount, payment_type, payment_installments |
| transactions_3 | `dbx_retail.bronze.transactions_3` | 2,749 | 8 | Order_ID, Product_ID, Sales, Quantity, discount, profit, payment_type, payment_installments |
| vendors | `dbx_retail.bronze.vendors` | 7 | 2 | vendor_id, vendor_name |

**Key observations:**
- All columns across all tables are stored as **STRING type** (no native numeric, date, or boolean types) — casting will be required during silver-layer transformation.
- The only exception is `products.product_photos_qty` which is LONG type.

---

## 3. Detailed Schema Per Table

### 3.1 customers_1 (87 rows, 9 columns)
| Column | Type | Description |
|---|---|---|
| customer_id | string | Primary key, format: "AB-10060" (2 letters, dash, 5 digits) |
| customer_email | string | Email address |
| customer_name | string | Full name |
| segment | string | Business segment: Consumer, Corporate, Home Office |
| country | string | Always "United States" |
| city | string | City name |
| state | string | US state name |
| postal_code | string | ZIP code |
| region | string | Geographic region (this table only contains "East") |

### 3.2 customers_2 (112 rows, 9 columns)
| Column | Type | Description |
|---|---|---|
| CustomerID | string | Primary key (same format as customer_id: "XX-NNNNN") — **PascalCase naming** |
| customer_email | string | Email address |
| customer_name | string | Full name |
| segment | string | Consumer, Corporate, Home Office |
| country | string | Country |
| city | string | City |
| state | string | State |
| postal_code | string | ZIP code |
| region | string | Geographic region |

### 3.3 customers_3 (53 rows, 9 columns)
| Column | Type | Description |
|---|---|---|
| cust_id | string | Primary key (same format) — **abbreviated naming** |
| customer_email | string | Email address (**20.75% missing/empty**) |
| customer_name | string | Full name |
| segment | string | Consumer, Corporate, Home Office |
| country | string | Country |
| city | string | City |
| state | string | State |
| postal_code | string | ZIP code |
| region | string | Geographic region |

### 3.4 customers_4 (80 rows, 9 columns)
| Column | Type | Description |
|---|---|---|
| customer_id | string | Primary key |
| customer_email | string | Email address |
| customer_name | string | Full name |
| segment | string | Consumer, Corporate, Home Office |
| country | string | Country |
| state | string | State — **NOTE: column order is swapped vs other tables (state before city)** |
| city | string | City |
| postal_code | string | ZIP code |
| region | string | Geographic region |

### 3.5 customers_5 (42 rows, 8 columns)
| Column | Type | Description |
|---|---|---|
| customer_id | string | Primary key |
| customer_name | string | Full name |
| segment | string | Consumer, Corporate, Home Office |
| country | string | Country |
| city | string | City |
| state | string | State |
| postal_code | string | ZIP code |
| region | string | Geographic region |

**NOTE:** This table is **missing the `customer_email` column entirely** (only 8 columns vs 9).

### 3.6 customers_6 (374 rows, 9 columns)
| Column | Type | Description |
|---|---|---|
| customer_identifier | string | Primary key — **completely different naming** |
| email_address | string | Email — renamed from customer_email (**11.23% missing**) |
| full_name | string | Name — renamed from customer_name |
| customer_segment | string | Segment — renamed from segment |
| country | string | Country |
| city | string | City |
| state | string | State |
| postal_code | string | ZIP code |
| region | string | Geographic region |

**NOTE:** This is the largest customer table (374 of 748 total customers = 50%). Uses entirely different column naming convention.

### 3.7 orders_1 (2,002 rows, 10 columns)
| Column | Type | Description |
|---|---|---|
| order_id | string | Primary key, format: "CA-2017-138422" or "US-2016-114293" |
| customer_id | string | FK → customers (all variants) |
| vendor_id | string | FK → vendors, format: "VEN01"-"VEN04" |
| ship_mode | string | Standard Class, Second Class, First Class, Same Day (**1.05% null**) |
| order_status | string | delivered, shipped, unavailable, canceled, processing, invoiced, created |
| order_purchase_date | string | Date as string, format: "MM/DD/YYYY HH:MM" (**0.35% null**) |
| order_approved_at | string | Approval timestamp |
| order_delivered_carrier_date | string | Carrier delivery timestamp (**1.9% null**) |
| order_delivered_customer_date | string | Customer delivery timestamp (**3.25% null**) |
| order_estimated_delivery_date | string | Estimated delivery date |

### 3.8 orders_2 (1,752 rows, 10 columns)
Same schema as orders_1 with these differences:
- `ship_mode`: **11.42% null** (200 rows) — significantly higher than orders_1
- `order_delivered_carrier_date`: 1.14% null
- `order_delivered_customer_date`: 2.51% null
- `order_approved_at`: 0.11% null

### 3.9 orders_3 (1,252 rows, 9 columns)
Same as orders_1 **EXCEPT**:
- **MISSING `order_estimated_delivery_date` column** (only 9 columns)
- `order_delivered_carrier_date`: 1.68% null
- `order_delivered_customer_date`: 3.27% null
- `ship_mode`: 0.8% null

### 3.10 products (1,774 rows, 13 columns)
| Column | Type | Description |
|---|---|---|
| brand | string | Brand name (e.g., "Calvin Klein", "Floafers", "Chaco") |
| categories | string | Comma-separated category hierarchy |
| colors | string | Product color |
| dateAdded | string | ISO 8601 timestamp (e.g., "2016-04-01T20:22:54Z") |
| dateUpdated | string | ISO 8601 timestamp |
| dimension | string | Product dimensions (**87.6% null**) |
| manufacturer | string | Manufacturer name (**59.02% null**) |
| product_id | string | Primary key, format: "FUR-BO-10001798" (Category prefix-subcategory-number) |
| product_name | string | Product description |
| product_photos_qty | long | Number of product photos (only non-string column in entire dataset) |
| sizes | string | Comma-separated available sizes (**65.56% null**) |
| upc | string | UPC barcode (**39.01% null**) |
| weight | string | Product weight (**94.36% null**) |

**NOTE:** The products table has very high sparsity. 5 of 13 columns have >39% missing data.

### 3.11 returns_1 (500 rows, 5 columns)
| Column | Type | Description |
|---|---|---|
| order_id | string | FK → orders |
| refund_amount | string | Refund value (**16.6% of rows have `$` prefix**, rest are plain numbers) |
| return_date | string | Date format: "YYYY-MM-DD" |
| return_reason | string | 10 distinct reasons (see categorical analysis below) |
| return_status | string | 6 values including abbreviations: Pending, Rejected, Approved, PENDG, APPRVD, RJCTD |

### 3.12 returns_2 (300 rows, 5 columns)
| Column | Type | Description |
|---|---|---|
| OrderId | string | FK → orders — **PascalCase naming** |
| amount | string | Refund value (clean, no `$` prefix) |
| date_of_return | string | Date — **different column name from returns_1** |
| reason | string | 11 distinct reasons (includes "?" placeholder = 35 rows / 11.7%) |
| status | string | 3 clean values: Pending, Rejected, Approved |

### 3.13 transactions_1 (3,730 rows, 8 columns)
| Column | Type | Description |
|---|---|---|
| Order_id | string | FK → orders (note: mixed casing "Order_id") |
| Product_id | string | FK → products |
| Sales | string | Sale amount (**20.5% of rows have `$` prefix**) |
| Quantity | string | Units sold |
| discount | string | Discount rate (decimal, e.g., "0.2") |
| profit | string | Profit/loss amount (can be negative) |
| payment_type | string | credit_card, voucher, debit_card, "?" placeholder |
| payment_installments | string | Number of installments |

### 3.14 transactions_2 (3,337 rows, 7 columns)
Same as transactions_1 **EXCEPT**:
- **MISSING `profit` column** (only 7 columns)
- `Order_id` casing: "Order_id" (same as transactions_1)
- `Sales`: Clean (no `$` prefix)
- `payment_type`: 1.26% null

### 3.15 transactions_3 (2,749 rows, 8 columns)
Same as transactions_1 **EXCEPT**:
- Column casing differs: `Order_ID`, `Product_ID` (uppercase "ID" vs "id")
- `Sales`: Clean (no `$` prefix)
- `profit`: Clean (no `$` prefix)
- `payment_type`: 1.67% null

### 3.16 vendors (7 rows, 2 columns)
| Column | Type | Description |
|---|---|---|
| vendor_id | string | Primary key, format: "VEN01" through "VEN07" |
| vendor_name | string | Vendor company name |

---

## 4. Schema Inconsistencies Across Related Tables

This section highlights column naming and structural differences that must be resolved during silver-layer harmonization.

### 4.1 Customer Tables — Column Mapping

| Canonical Column | customers_1 | customers_2 | customers_3 | customers_4 | customers_5 | customers_6 |
|---|---|---|---|---|---|---|
| customer_id (PK) | customer_id | CustomerID | cust_id | customer_id | customer_id | customer_identifier |
| email | customer_email | customer_email | customer_email | customer_email | **MISSING** | email_address |
| name | customer_name | customer_name | customer_name | customer_name | customer_name | full_name |
| segment | segment | segment | segment | segment | segment | customer_segment |
| country | country | country | country | country | country | country |
| city | city | city | city | city* | city | city |
| state | state | state | state | state* | state | state |
| postal_code | postal_code | postal_code | postal_code | postal_code | postal_code | postal_code |
| region | region | region | region | region | region | region |

*In customers_4, `state` and `city` column order is swapped (state appears before city), though the data values are correct.

**Key issues:**
- 4 different names for the customer ID column
- customers_5 has no email column at all
- customers_6 uses entirely different naming convention (full_name, email_address, customer_segment, customer_identifier)

### 4.2 Order Tables — Column Mapping

| Column | orders_1 | orders_2 | orders_3 |
|---|---|---|---|
| order_id | ✓ | ✓ | ✓ |
| customer_id | ✓ | ✓ | ✓ |
| vendor_id | ✓ | ✓ | ✓ |
| ship_mode | ✓ | ✓ | ✓ |
| order_status | ✓ | ✓ | ✓ |
| order_purchase_date | ✓ | ✓ | ✓ |
| order_approved_at | ✓ | ✓ | ✓ |
| order_delivered_carrier_date | ✓ | ✓ | ✓ |
| order_delivered_customer_date | ✓ | ✓ | ✓ |
| order_estimated_delivery_date | ✓ | ✓ | **MISSING** |

**Key issue:** orders_3 is missing `order_estimated_delivery_date`.

### 4.3 Returns Tables — Column Mapping

| Canonical Column | returns_1 | returns_2 |
|---|---|---|
| order_id (FK) | order_id | OrderId |
| refund_amount | refund_amount | amount |
| return_date | return_date | date_of_return |
| return_reason | return_reason | reason |
| return_status | return_status | status |

**Key issue:** Every single column has a different name between the two tables.

### 4.4 Transactions Tables — Column Mapping

| Canonical Column | transactions_1 | transactions_2 | transactions_3 |
|---|---|---|---|
| order_id (FK) | Order_id | Order_id | Order_ID |
| product_id (FK) | Product_id | Product_id | Product_ID |
| sales | Sales | Sales | Sales |
| quantity | Quantity | Quantity | Quantity |
| discount | discount | discount | discount |
| profit | profit | **MISSING** | profit |
| payment_type | payment_type | payment_type | payment_type |
| payment_installments | payment_installments | payment_installments | payment_installments |

**Key issues:**
- transactions_2 is missing the `profit` column
- Column casing varies: `Order_id` vs `Order_ID`, `Product_id` vs `Product_ID`

---

## 5. Missing Data (Null / Empty) Analysis

Ordered by percentage missing (descending). Only columns with >0% missing are listed.

| Table | Column | Null/Empty Count | Total Rows | % Missing |
|---|---|---|---|---|
| products | weight | 1,674 | 1,774 | 94.36% |
| products | dimension | 1,554 | 1,774 | 87.60% |
| products | sizes | 1,163 | 1,774 | 65.56% |
| products | manufacturer | 1,047 | 1,774 | 59.02% |
| products | upc | 692 | 1,774 | 39.01% |
| customers_3 | customer_email | 11 | 53 | 20.75% |
| orders_2 | ship_mode | 200 | 1,752 | 11.42% |
| customers_6 | email_address | 42 | 374 | 11.23% |
| orders_3 | order_delivered_customer_date | 41 | 1,252 | 3.27% |
| orders_1 | order_delivered_customer_date | 65 | 2,002 | 3.25% |
| orders_2 | order_delivered_customer_date | 44 | 1,752 | 2.51% |
| orders_1 | order_delivered_carrier_date | 38 | 2,002 | 1.90% |
| orders_3 | order_delivered_carrier_date | 21 | 1,252 | 1.68% |
| transactions_3 | payment_type | 46 | 2,749 | 1.67% |
| transactions_2 | payment_type | 42 | 3,337 | 1.26% |
| orders_2 | order_delivered_carrier_date | 20 | 1,752 | 1.14% |
| orders_1 | ship_mode | 21 | 2,002 | 1.05% |
| orders_3 | ship_mode | 10 | 1,252 | 0.80% |
| orders_1 | order_approved_at | 7 | 2,002 | 0.35% |
| orders_2 | order_approved_at | 2 | 1,752 | 0.11% |
| orders_3 | order_approved_at | 1 | 1,252 | 0.08% |

**Summary:**
- **Products table** is the most sparse: weight (94%), dimension (88%), sizes (66%), manufacturer (59%), upc (39%)
- **Customer emails** have notable gaps: customers_3 (21%), customers_6 (11%), customers_5 (100% — column doesn't exist)
- **Order delivery dates** have 1-3% nulls (likely undelivered orders)
- **ship_mode** in orders_2 has 11.4% null — anomalously high compared to orders_1 (1%) and orders_3 (0.8%)

---

## 6. Duplicate Key Analysis

| Table | Key Column | Total Rows | Distinct Keys | Duplicate Rows | % Duplicates |
|---|---|---|---|---|---|
| customers_1 | customer_id | 87 | 87 | 0 | 0.00% |
| customers_2 | CustomerID | 112 | 112 | 0 | 0.00% |
| customers_3 | cust_id | 53 | 53 | 0 | 0.00% |
| customers_4 | customer_id | 80 | 80 | 0 | 0.00% |
| customers_5 | customer_id | 42 | 42 | 0 | 0.00% |
| customers_6 | customer_identifier | 374 | 374 | 0 | 0.00% |
| orders_1 | order_id | 2,002 | 2,002 | 0 | 0.00% |
| orders_2 | order_id | 1,752 | 1,752 | 0 | 0.00% |
| orders_3 | order_id | 1,252 | 1,252 | 0 | 0.00% |
| products | product_id | 1,774 | 1,774 | 0 | 0.00% |
| returns_1 | order_id | 500 | 500 | 0 | 0.00% |
| returns_2 | OrderId | 300 | 300 | 0 | 0.00% |
| transactions_1 | Order_id | 3,730 | 2,703 | 1,027 | 27.53% |
| transactions_2 | Order_id | 3,337 | 2,478 | 859 | 25.74% |
| transactions_3 | Order_ID | 2,749 | 2,155 | 594 | 21.61% |
| vendors | vendor_id | 7 | 7 | 0 | 0.00% |

**Summary:**
- All customer, order, product, return, and vendor tables have **unique primary keys** — no duplicates.
- **Transaction tables have 22-28% "duplicates" on Order_id** — this is expected behavior since transactions are order line items (multiple products per order). The natural key for transactions is `(Order_id, Product_id)`, not Order_id alone.

---

## 7. Data Quality Issues

### 7.1 Format Inconsistencies in Numeric String Columns

| Table | Column | Total Rows | With `$` Prefix | Without `$` | Status |
|---|---|---|---|---|---|
| transactions_1 | Sales | 3,730 | 763 | 2,967 | **20.5% have $ prefix** |
| transactions_1 | profit | 3,730 | 0 | 3,730 | Clean |
| transactions_2 | Sales | 3,337 | 0 | 3,337 | Clean |
| transactions_3 | Sales | 2,749 | 0 | 2,749 | Clean |
| transactions_3 | profit | 2,749 | 0 | 2,749 | Clean |
| returns_1 | refund_amount | 500 | 83 | 417 | **16.6% have $ prefix** |
| returns_2 | amount | 300 | 0 | 300 | Clean |

**Action required:** Strip `$` prefix from `transactions_1.Sales` and `returns_1.refund_amount` before casting to numeric.

### 7.2 All Columns Are STRING Type

Every column across all 16 tables is stored as string type (except `products.product_photos_qty` which is LONG). During silver-layer transformation, the following casts are needed:

| Data Type Needed | Columns |
|---|---|
| DECIMAL/DOUBLE | Sales, profit, discount, refund_amount/amount, Quantity, payment_installments |
| DATE | return_date, date_of_return, order_estimated_delivery_date |
| TIMESTAMP | order_purchase_date, order_approved_at, order_delivered_carrier_date, order_delivered_customer_date, dateAdded, dateUpdated |
| INTEGER | Quantity, payment_installments, product_photos_qty (already LONG) |

### 7.3 Dirty/Placeholder Values in Categorical Columns

**`?` placeholder values found in:**
- `transactions_1.payment_type`: 357 rows (9.6% of 3,730)
- `returns_2.reason`: 35 rows (11.7% of 300)

These need to be mapped to NULL or a proper "Unknown" category.

### 7.4 Inconsistent Abbreviations in return_status

`returns_1.return_status` has both full names and abbreviations:

| Value | Count | Canonical Form |
|---|---|---|
| Pending | 153 | Pending |
| Rejected | 146 | Rejected |
| Approved | 136 | Approved |
| PENDG | 24 | → Pending |
| APPRVD | 21 | → Approved |
| RJCTD | 20 | → Rejected |

`returns_2.status` is clean (only Pending: 110, Rejected: 107, Approved: 83).

---

## 8. Categorical Value Distributions

### 8.1 Customer Segment (from customers_1)
| Segment | Count |
|---|---|
| Consumer | 43 |
| Corporate | 26 |
| Home Office | 18 |

### 8.2 Customer Region (from customers_1)
| Region | Count |
|---|---|
| East | 87 |

Note: customers_1 only contains East region. Other regions are distributed across other customer tables.

### 8.3 Order Status (from orders_1)
| Status | Count | % |
|---|---|---|
| delivered | 1,937 | 96.8% |
| shipped | 24 | 1.2% |
| unavailable | 12 | 0.6% |
| canceled | 12 | 0.6% |
| processing | 10 | 0.5% |
| invoiced | 6 | 0.3% |
| created | 1 | 0.05% |

### 8.4 Ship Mode (from orders_1)
| Ship Mode | Count | % |
|---|---|---|
| Standard Class | 1,170 | 58.4% |
| Second Class | 373 | 18.6% |
| First Class | 330 | 16.5% |
| Same Day | 108 | 5.4% |
| NULL | 21 | 1.0% |

### 8.5 Payment Type (from transactions_1)
| Payment Type | Count | % |
|---|---|---|
| credit_card | 3,343 | 89.6% |
| ? (placeholder) | 357 | 9.6% |
| voucher | 22 | 0.6% |
| debit_card | 8 | 0.2% |

### 8.6 Return Reason (from returns_1)
| Reason | Count |
|---|---|
| Not Satisfied | 173 |
| Damaged | 113 |
| Wrong Delivery | 60 |
| Changed Mind | 33 |
| Wrong Item Received | 28 |
| Defective Product | 25 |
| Late Delivery | 23 |
| Quality Not As Described | 19 |
| Missing Parts | 16 |
| Duplicate Order | 10 |

### 8.7 Return Reason (from returns_2)
| Reason | Count |
|---|---|
| Defective Product | 41 |
| ? (placeholder) | 35 |
| Late Delivery | 33 |
| Quality Not As Described | 29 |
| Changed Mind | 29 |
| Damaged | 28 |
| Wrong Delivery | 27 |
| Wrong Item Received | 24 |
| Duplicate Order | 21 |
| Missing Parts | 18 |
| Not Satisfied | 15 |

---

## 9. Cross-Table Referential Integrity

| Relationship | Source Distinct Keys | Matched in Target | Orphaned Keys |
|---|---|---|---|
| Orders.customer_id → Customers (all 6 tables) | 332 | 332 | **0** |
| Orders.vendor_id → Vendors | 4 | 4 | **0** |
| Transactions.order_id → Orders (all 3 tables) | 4,960 | 4,960 | **0** |
| Returns.order_id → Orders (all 3 tables) | 800 | 800 | **0** |
| Transactions.product_id → Products | 1,774 | 1,774 | **0** |

**All foreign key relationships are fully intact with zero orphaned keys.** Every customer, vendor, order, and product referenced in transactional tables exists in the corresponding master table.

**Additional observations:**
- Only 4 of 7 vendors are referenced in orders (VEN01-VEN04)
- 332 of 748 distinct customers have placed orders (44.4%)
- All 1,774 products appear in transactions

---

## 10. Entity Relationship Model

```
┌─────────────┐     ┌─────────────┐     ┌──────────────┐
│  Customers  │     │   Vendors   │     │   Products   │
│ (6 tables)  │     │ (1 table)   │     │  (1 table)   │
│  748 rows   │     │   7 rows    │     │  1,774 rows  │
│  PK: varies │     │ PK:vendor_id│     │PK:product_id │
└──────┬──────┘     └──────┬──────┘     └──────┬───────┘
       │                   │                    │
       │ customer_id       │ vendor_id          │ product_id
       │                   │                    │
       ▼                   ▼                    ▼
┌──────────────────────────────────┐    ┌───────────────────┐
│            Orders                │    │   Transactions    │
│         (3 tables)               │◄───│    (3 tables)     │
│         5,006 rows               │    │   9,816 rows      │
│     PK: order_id                 │    │ FK: Order_id      │
│     FK: customer_id, vendor_id   │    │ FK: Product_id    │
└──────────────┬───────────────────┘    └───────────────────┘
               │
               │ order_id
               ▼
      ┌─────────────────┐
      │     Returns     │
      │   (2 tables)    │
      │    800 rows     │
      │  FK: order_id   │
      └─────────────────┘
```

---

## 11. Recommended Silver-Layer Transformations

Based on this profiling, the following transformations are recommended:

1. **Harmonize column names** across all table groups using the canonical mappings in Section 4.
2. **Union related tables** within each group (customers_1-6, orders_1-3, etc.) after harmonization.
3. **Strip `$` prefix** from `transactions_1.Sales` and `returns_1.refund_amount` before numeric casting.
4. **Cast string columns to proper types** (DECIMAL, DATE, TIMESTAMP, INTEGER) as outlined in Section 7.2.
5. **Standardize return_status abbreviations**: Map PENDG→Pending, APPRVD→Approved, RJCTD→Rejected.
6. **Replace `?` placeholders** with NULL in `payment_type` and `return_reason`.
7. **Handle missing profit column** in transactions_2 — either exclude from profit analysis or fill with NULL.
8. **Handle missing order_estimated_delivery_date** in orders_3 — fill with NULL.
9. **Handle missing customer_email** in customers_5 — fill with NULL.
10. **Validate date formats** across order tables ("MM/DD/YYYY HH:MM" format) and return tables ("YYYY-MM-DD" format) before standardizing.

---

## 12. Data Volume Summary

| Metric | Value |
|---|---|
| Total tables | 16 |
| Total rows | ~16,151 |
| Total unique customers | 748 |
| Total unique orders | 5,006 |
| Total transaction line items | 9,816 |
| Total returns | 800 |
| Total products | 1,774 |
| Total vendors | 7 |
| Customers with orders | 332 (44.4%) |
| Active vendors | 4 of 7 (57.1%) |
| Return rate (orders with returns) | 800 / 5,006 = 16.0% |
