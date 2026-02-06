📊 Revanta — Power BI Dashboard Build Guide

Estimated Time: 30–45 minutes
Difficulty: Beginner → Intermediate
Outcome: Production-ready analytics dashboard (Executive + Analytics)

This guide walks you through building the Revanta Retail Analytics Dashboard using the data produced by the ETL pipeline.

🔗 Step 1: Connect to Data 
✅ Option A — Connect to SQLite (Recommended)

Open Power BI Desktop

Click Get Data → More…

Select Database → SQLite

Browse to:

database/revanta.db


Select the following tables:

analytics_customer_rfm

analytics_customer_risk_scoring

analytics_monthly_revenue

dim_customers

Click Load

⚠️ If the SQLite connector is not available, install the SQLite ODBC driver
or use Option B below.

🔁 Option B — Connect via CSV (Fallback / Publishing-Friendly)

If SQLite is unavailable or you plan to publish to Power BI Service, load CSVs instead:

bi_exports/


This option guarantees compatibility and avoids local driver issues.

📐 Step 2: Data Preparation & Modeling 
Power Query Checks

Click Transform Data

Ensure correct data types:

Dates → Date

Revenue & metrics → Decimal Number

Click Close & Apply

Relationships (Model View)

Create the following relationships:

From Table	Column	To Table	Column	Type
analytics_customer_rfm	customer_sk	dim_customers	customer_sk	One-to-Many
analytics_customer_risk_scoring	customer_sk	dim_customers	customer_sk	One-to-Many
analytics_monthly_revenue	customer_sk	dim_customers	customer_sk	One-to-Many

📌 Verify relationships visually in Model View

Best Practice Notes:

Dimensions filter facts

Avoid bidirectional filters

Single shared customer dimension

📐 Step 3: Core Measures 

Create these once and reuse across all pages.

Total Customers =
DISTINCTCOUNT(dim_customers[customer_sk])

Avg Risk Score =
AVERAGE(analytics_customer_risk_scoring[risk_score])

Critical Customers =
CALCULATE(
    DISTINCTCOUNT(analytics_customer_risk_scoring[customer_sk]),
    analytics_customer_risk_scoring[risk_category] = "CRITICAL"
)

Total Revenue =
SUM(analytics_monthly_revenue[total_revenue])

Avg Order Value =
AVERAGE(analytics_monthly_revenue[avg_order_value])


✅ These 5 measures power the entire dashboard

🎨 Step 4: Build Dashboard Pages 
📌 PAGE 1 — Executive Summary

Purpose: High-level health & risk overview

KPI Cards

Total Customers

Avg Risk Score

Critical Customers

Total Revenue

Visuals

Gauge: Avg Risk Score (0 → 1)

Pie Chart: Customer distribution by rfm_segment

Slicer

Risk Category (Button style)

Apply to all pages

📌 PAGE 2 — Customer Risk Analysis

Risk Distribution

Column chart

X-axis: risk_score (create bins: 0–0.2, 0.2–0.4, etc.)

Y-axis: Customer count

Risk Categories

Stacked bar chart

CRITICAL → VERY_LOW (Red → Green)

Detail Table

customer_id

days_since_purchase

risk_score

risk_category

Sort by risk_score (descending)

📌 PAGE 3 — RFM Segmentation

Segment Distribution

Pie chart: Customers by RFM segment

Segment Performance

Matrix:

Segment

Customer count

Avg lifetime value

Avg order value

Segment Table

Segment-level metrics for analysis

📌 PAGE 4 — Revenue Trends

Revenue Over Time

Line chart

X-axis: year_month

Y-axis: total_revenue

Top Customers

Table: Top 10 customers by lifetime value

KPIs

Current Month Revenue

Month-over-Month Growth %

🎨 Step 5: Formatting & UX Standards 
Color Palette

Risk: Red → Green

Primary: Blue

Background: Light gray

Typography

Font: Segoe UI

Titles: 18–20pt (Bold)

Labels: 10–11pt

Layout Rules

Align visuals

One insight per visual

Avoid clutter

Consistent spacing

✅ Step 6: Validation Checklist (10 minutes)
☑ Total Customers ≈ 99,441
☑ Risk categories distribute correctly
☑ Slicers affect all visuals
☑ No broken relationships
☑ Dashboard loads < 5 seconds

💾 Step 7: Save & Store

Save locally as:

dashboard/revanta.pbix