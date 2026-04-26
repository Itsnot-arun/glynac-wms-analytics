# Glynac WMS — Referral Analytics Pipeline
**Springer Capital | Data Engineering Intern Take-Home Test**
**Author:** Arun K | **Period:** Apr–Jul 2026

---

## 📋 Project Overview

This project builds a data pipeline that:
1. **Profiles** all 7 source CSV tables (null counts, distinct values)
2. **Cleans & transforms** the data (timezone conversion, deduplication, initcap)
3. **Joins** all tables into a unified referral report
4. **Detects potential fraud** using business logic rules (`is_business_logic_valid`)
5. **Outputs** a 46-row CSV report with all required columns

---

## 📁 Project Structure

```
glynac-wms-analytics/
├── pipeline.py              # Main ETL + fraud detection script
├── Dockerfile               # Container definition
├── requirements.txt         # Python dependencies
├── README.md                # This file
├── data/                    # Source CSV files (7 tables)
│   ├── lead_log.csv
│   ├── paid_transactions.csv
│   ├── referral_rewards.csv
│   ├── user_logs.csv
│   ├── user_referral_logs.csv
│   ├── user_referral_statuses.csv
│   └── user_referrals.csv
├── output/                  # Generated outputs (after running)
│   ├── referral_report.csv       # Final 46-row fraud-validated report
│   └── data_profiling_report.csv # Profiling report
└── docs/
    └── data_dictionary.xlsx      # Business data dictionary
```

---

## 🚀 Option A: Run with Docker (Recommended)

### Prerequisites
- [Docker](https://www.docker.com/get-started) installed

### Steps

```bash
# 1. Clone the repository
git clone https://github.com/itsnot-arun/glynac-wms-analytics.git
cd glynac-wms-analytics

# 2. Build the Docker image
docker build -t glynac-pipeline .

# 3. Run the pipeline and export output to your local machine
docker run --rm -v $(pwd)/output:/app/output glynac-pipeline
```

> On Windows (Command Prompt):
> ```cmd
> docker run --rm -v %cd%/output:/app/output glynac-pipeline
> ```

### Output
After running, check the `output/` folder:
- `referral_report.csv` — final report (46 rows)
- `data_profiling_report.csv` — profiling results

---

## 🐍 Option B: Run Locally with Python

### Prerequisites
- Python 3.9+
- pip

### Steps

```bash
# 1. Clone the repository
git clone https://github.com/itsnot-arun/glynac-wms-analytics.git
cd glynac-wms-analytics

# 2. Install dependencies
pip install -r requirements.txt

# 3. Run the pipeline
python pipeline.py
```

---

## 📊 Pipeline Steps

| Step | Description |
|------|-------------|
| 1. Load | Read all 7 CSV files into DataFrames |
| 2. Profile | Compute null counts and distinct value counts per column |
| 3. Clean | Parse timestamps, extract reward values, apply initcap, deduplicate |
| 4. Process | Timezone conversion (UTC → Asia/Jakarta), source category logic, table joins |
| 5. Validate | Apply 7 business logic rules to detect fraud (`is_business_logic_valid`) |
| 6. Output | Save 46-row report CSV + profiling report CSV |

---

## 🔍 Business Logic — Fraud Detection

The `is_business_logic_valid` column flags each referral as valid or potentially fraudulent:

**VALID conditions:**
- `V1`: Status = Berhasil + reward > 0 + PAID NEW transaction after referral in same month + active referrer + reward granted
- `V2`: Status = Menunggu/Tidak Berhasil + no reward assigned

**INVALID conditions (fraud flags):**
- `I1`: Reward > 0 but status is not Berhasil
- `I2`: Reward > 0 but no transaction ID
- `I3`: No reward but has PAID transaction after referral
- `I4`: Status is Berhasil but reward is null/0
- `I5`: Transaction occurred BEFORE referral creation

---

## 📤 Output Report Columns

| Column | Type | Description |
|--------|------|-------------|
| referral_details_id | INTEGER | Auto-incremented row ID |
| referral_id | STRING | Unique referral identifier |
| referral_source | STRING | User Sign Up / Draft Transaction / Lead |
| referral_source_category | STRING | Online / Offline |
| referral_at | DATETIME | Referral creation time (Jakarta) |
| referrer_id | STRING | ID of the referring member |
| referrer_name | STRING | Name of referrer |
| referrer_phone_number | STRING | Phone of referrer |
| referrer_homeclub | STRING | Gym branch of referrer |
| referee_id | STRING | ID of referred member |
| referee_name | STRING | Name of referred member |
| referee_phone | STRING | Phone of referred member |
| referral_status | STRING | Berhasil / Menunggu / Tidak Berhasil |
| num_reward_days | INTEGER | Reward days (10, 15, or 20) |
| transaction_id | STRING | Linked transaction ID |
| transaction_status | STRING | Paid / null |
| transaction_at | DATETIME | Transaction time (Jakarta) |
| transaction_location | STRING | Gym branch of transaction |
| transaction_type | STRING | New / Renewal |
| updated_at | DATETIME | Last update time (Jakarta) |
| reward_granted_at | DATETIME | When reward was granted |
| is_business_logic_valid | BOOLEAN | TRUE = valid, FALSE = potential fraud |

---

## 📚 Documentation
- **Data Dictionary**: `docs/data_dictionary.xlsx` — business-friendly definitions for all columns, business logic rules, and profiling results

---

*Glynac / Springer Capital | Data Engineering Intern Assessment | Arun K | April 2026*
