# Glynac WMS — Referral Analytics Pipeline

A data engineering project that ingests raw CSV exports from the Glynac Warehouse Management System and transforms them into an interactive analytics dashboard.

## 📊 Live Dashboard

[View Live on Vercel →](https://your-vercel-url.vercel.app)

## 🏗️ Architecture

```
Raw CSVs (7 tables)
    │
    ▼
pipeline/etl.py  ←── Extract, Transform, Load
    │
    ▼
public/analytics.json  ←── Analytics-ready JSON
    │
    ▼
public/index.html  ←── Interactive Dashboard (Chart.js)
```

## 📁 Data Sources

| Table | Records | Description |
|-------|---------|-------------|
| `user_referrals` | 46 | Referral relationships between members |
| `user_referral_logs` | 96 | Referral event log |
| `user_logs` | 29 | Member records |
| `paid_transactions` | 14 | Payment events |
| `lead_log` | 8 | Sales leads |
| `referral_rewards` | 3 | Reward tiers (10/15/20 days) |
| `user_referral_statuses` | 3 | Status lookup (Pending/Success/Failed) |

## 📈 Key Metrics

- **46** total referrals across Mar–May 2024
- **17.4%** referral conversion rate
- **17** rewards successfully granted (17.7% rate)
- **28** active members across 6 gym locations

## 🛠️ Tech Stack

- **ETL Pipeline**: Python, Pandas
- **Dashboard**: HTML, Chart.js, Vanilla JS
- **Deployment**: Vercel (static)

## 🚀 Run Locally

```bash
# Install dependencies
pip install pandas

# Run ETL pipeline
python pipeline/etl.py

# Serve dashboard
cd public && python -m http.server 3000
# Open http://localhost:3000
```

## 📂 Project Structure

```
glynac-data-project/
├── data/                    # Raw CSV source files
│   ├── lead_log.csv
│   ├── paid_transactions.csv
│   ├── referral_rewards.csv
│   ├── user_logs.csv
│   ├── user_referral_logs.csv
│   ├── user_referral_statuses.csv
│   └── user_referrals.csv
├── pipeline/
│   └── etl.py               # ETL transformation script
├── public/
│   ├── index.html           # Analytics dashboard
│   └── analytics.json       # Generated metrics
└── README.md
```

---

**Intern:** Arun K | **Org:** Glynac / Springer Capital | **Period:** Apr–Jul 2026
