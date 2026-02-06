🚀 Revanta — Retail Analytics Platform

An end-to-end analytics engineering project delivering customer RFM segmentation, churn risk scoring, and an interactive Power BI dashboard for retail e-commerce.

Built with a production-style ETL pipeline, SQL data marts, and BI-ready exports.

🧠 Project Overview

Revanta transforms raw transactional data into business-ready analytics that answer:

Who are my most valuable customers?

Which customers are at risk of churn?

How is revenue trending over time?

Where should marketing focus retention efforts?

This project follows modern analytics engineering principles:

Clean separation of extract → transform → load

SQL-driven dimensional modeling

BI-friendly outputs for Power BI

⚡ Quick Start (5 Minutes)
# 1. Clone repository
git clone https://github.com/yourusername/revanta.git
cd revanta

# 2. Create virtual environment
python -m venv venv
source venv/bin/activate      # Windows: venv\Scripts\activate

# 3. Install dependencies
pip install -r requirements.txt

# 4. Run full pipeline
python etl/run_pipeline.py

📊 Build the Dashboard

Open Power BI Desktop

Load CSV files from: bi_exports/

Follow the guide: dashboard/power_bi_model.md

✅ Data is now BI-ready.

🏗️ Architecture

Flow:

Raw CSV Data
    ↓
Extract (Python)
    ↓
Transform (Python – hygiene & validation)
    ↓
Load (SQLite – staging layer)
    ↓
SQL Transformations
    ↓
Dimensions & Facts
    ↓
Analytics Tables
    ↓
Power BI Dashboard

🎯 Key Features
Feature	Description
RFM Segmentation	8 customer segments based on behavior
Risk Scoring	Identifies churn-risk customers
Automated ETL	One command, 2–5 min runtime
Analytics Marts	Star-schema facts & dimensions
Power BI Dashboard	4 interactive analytical views
Note: For analysis purposes, 'Today' is simulated as 2026 to evaluate historical retention patterns.
📁 Project Structure
REVANTA/
├── assets/                     # Architecture diagrams
├── bi_exports/                 # CSVs for Power BI
├── config/                     # YAML configuration
├── dashboard/                  # Power BI docs & screenshots
│   ├── screenshots/
│   └── power_bi_model.md
├── data/
│   ├── raw/                    # Original Kaggle CSVs
│   ├── processed/
│   └── sample/
├── database/
│   ├── revanta.db              # Generated SQLite DB
│   ├── schema.sql
│   └── run_sql_transformations.py
├── etl/
│   ├── extract.py
│   ├── transform.py
│   ├── load.py
│   ├── export_for_bi.py
│   └── run_pipeline.py
├── notebooks/
│   └── exploratory_analysis.ipynb
├── sql/
│   ├── staging/
│   ├── marts/
│   └── analysis/
├── README.md
└── requirements.txt

🔄 Data Pipeline
Extract

Reads raw CSV files from the Olist Brazilian E-Commerce Dataset

Preserves data unchanged for traceability

Transform

Column standardization

Type casting & validation

Duplicate removal

Grain enforcement (1 row = 1 entity)

Load

Writes clean data to SQLite staging tables

Acts as a lightweight data warehouse

SQL Transformations

Dimensions

dim_customers

dim_products

dim_date

Facts

fct_sales — order-level metrics

fct_order_items — line-item detail

Analytics

analytics_customer_rfm

analytics_customer_risk_scoring

analytics_monthly_revenue

Olist dataset: [https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce]

📊 Power BI Dashboard (4 Pages)

📸 See screenshots in dashboard/screenshots/

1️⃣ Executive Summary

Total customers

Average risk score

Segment distribution

KPI overview

2️⃣ Customer Risk Analysis

Risk distribution

At-risk customers

Risk category breakdown

3️⃣ RFM Segmentation

8 customer segments

Revenue contribution by segment

Customer value analysis

4️⃣ Monthly Revenue Trends

Revenue over time

Growth trends

Top customers by lifetime value

⏱️ Build time: ~45–60 minutes using the guide

📈 Business Impact

Identify 15–20% of customers at churn risk

Enable targeted retention campaigns

Optimize marketing spend via segmentation

Improve customer lifetime value (CLV)

Support data-driven decision making

🛠️ Tech Stack

Python — Pandas, logging, orchestration

SQLite — lightweight analytical warehouse

SQL — transformations & business logic

Power BI Desktop — visualization & reporting

YAML — configuration management

Git — version control

📚 Documentation

📊 Dashboard Guide: dashboard/power_bi_model.md

🗄️ Database Schema: database/schema.sql

⚙️ ETL Orchestration: etl/run_pipeline.py

🧮 SQL Logic: sql/

🚀 Next Steps

Run pipeline: python etl/run_pipeline.py

Build dashboard using guide

Publish dashboard to Power BI Service

Extend with new metrics or data sources

🔒 License

MIT — free to use, modify, and share.

👨‍💻 Author

Omar Awad
Data Engineer | Analytics Engineer
🔗 LinkedIn [www.linkedin.com/in/eng-omar-awad] | 🐙 GitHub [https://github.com/Omar-M-Awad]