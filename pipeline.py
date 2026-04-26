"""
=============================================================================
  Glynac WMS — Referral Analytics Pipeline
  Springer Capital | Data Engineering Intern Take-Home Test
  Author: Arun K
=============================================================================

  This script:
    1. Loads 7 source CSV tables
    2. Profiles all tables (null counts, distinct counts)
    3. Cleans and transforms data (timezone, initcap, deduplication)
    4. Joins tables into a unified referral report
    5. Applies fraud detection business logic
    6. Outputs a 46-row CSV report + profiling report
"""

# =============================================================================
#  IMPORTS
# =============================================================================
import os
import pandas as pd
import numpy as np
import pytz


# =============================================================================
#  CONFIGURATION
# =============================================================================
BASE_DIR   = os.path.dirname(os.path.abspath(__file__))
DATA_DIR   = os.path.join(BASE_DIR, 'data')
OUTPUT_DIR = os.path.join(BASE_DIR, 'output')
TIMEZONE   = pytz.timezone('Asia/Jakarta')

os.makedirs(OUTPUT_DIR, exist_ok=True)


# =============================================================================
#  HELPER FUNCTIONS
# =============================================================================
def log(msg, level=0):
    """Pretty-print a step header or message."""
    if level == 0:
        print('\n' + '=' * 70)
        print(f'  {msg}')
        print('=' * 70)
    else:
        print(f'  • {msg}')


def parse_timestamp(series):
    """Parse ISO 8601 timestamps to UTC-aware datetime."""
    return pd.to_datetime(series, utc=True, errors='coerce')


def to_local_time(series, tz=TIMEZONE):
    """Convert UTC datetime to local timezone (Jakarta), then strip tz info."""
    if series.dt.tz is None:
        series = series.dt.tz_localize('UTC')
    return series.dt.tz_convert(tz).dt.tz_localize(None)


def initcap(series):
    """Apply Title Case to a string column, leaving NaN intact."""
    return series.apply(lambda x: x.title() if isinstance(x, str) else x)


def format_datetime(series):
    """Format datetime as 'YYYY-MM-DD HH:MM:SS' string."""
    return series.apply(
        lambda x: x.strftime('%Y-%m-%d %H:%M:%S') if pd.notna(x) else None
    )


# =============================================================================
#  STEP 1  —  LOAD DATA
# =============================================================================
def load_data():
    """Load all 7 source CSV files into DataFrames."""
    log('STEP 1: Loading data')

    tables = {
        'lead_log':               pd.read_csv(f'{DATA_DIR}/lead_log.csv'),
        'paid_transactions':      pd.read_csv(f'{DATA_DIR}/paid_transactions.csv'),
        'referral_rewards':       pd.read_csv(f'{DATA_DIR}/referral_rewards.csv'),
        'user_logs':              pd.read_csv(f'{DATA_DIR}/user_logs.csv'),
        'user_referral_logs':     pd.read_csv(f'{DATA_DIR}/user_referral_logs.csv'),
        'user_referral_statuses': pd.read_csv(f'{DATA_DIR}/user_referral_statuses.csv'),
        'user_referrals':         pd.read_csv(f'{DATA_DIR}/user_referrals.csv'),
    }

    for name, df in tables.items():
        log(f'{name:25s} → {len(df):4d} rows', level=1)

    return tables


# =============================================================================
#  STEP 2  —  DATA PROFILING
# =============================================================================
def profile_tables(tables):
    """Generate null count and distinct value count for every column."""
    log('STEP 2: Data profiling')

    rows = []
    for table_name, df in tables.items():
        for col in df.columns:
            sample = df[col].dropna().iloc[0] if df[col].notna().any() else 'N/A'
            rows.append({
                'table_name':     table_name,
                'column_name':    col,
                'data_type':      str(df[col].dtype),
                'null_count':     int(df[col].isnull().sum()),
                'null_pct':       round(df[col].isnull().mean() * 100, 2),
                'distinct_count': int(df[col].nunique()),
                'sample_value':   str(sample),
            })

    profile_df = pd.DataFrame(rows)
    output_path = f'{OUTPUT_DIR}/data_profiling_report.csv'
    profile_df.to_csv(output_path, index=False)

    log(f'Profile saved → {output_path}', level=1)
    log(f'Total columns profiled: {len(profile_df)}', level=1)
    return profile_df


# =============================================================================
#  STEP 3  —  DATA CLEANING
# =============================================================================
def clean_data(tables):
    """Parse timestamps, extract numeric values, apply initcap, deduplicate."""
    log('STEP 3: Data cleaning')

    user_referrals    = tables['user_referrals']
    paid_transactions = tables['paid_transactions']
    user_ref_logs     = tables['user_referral_logs']
    user_logs         = tables['user_logs']
    lead_log          = tables['lead_log']
    referral_rewards  = tables['referral_rewards']
    user_ref_statuses = tables['user_referral_statuses']

    # Parse timestamps
    user_referrals['referral_at']        = parse_timestamp(user_referrals['referral_at'])
    user_referrals['updated_at']         = parse_timestamp(user_referrals['updated_at'])
    paid_transactions['transaction_at']  = parse_timestamp(paid_transactions['transaction_at'])
    user_ref_logs['created_at']          = parse_timestamp(user_ref_logs['created_at'])
    lead_log['created_at']               = parse_timestamp(lead_log['created_at'])
    user_logs['membership_expired_date'] = pd.to_datetime(
        user_logs['membership_expired_date'], errors='coerce', utc=True
    )

    # Extract numeric reward value: "10 days" → 10
    referral_rewards['reward_value_num'] = (
        referral_rewards['reward_value'].str.extract(r'(\d+)').astype(float)
    )

    # Apply initcap (title case) to string columns. Club names kept as-is.
    user_referrals['referee_name']          = initcap(user_referrals['referee_name'])
    user_referrals['referral_source']       = initcap(user_referrals['referral_source'])
    paid_transactions['transaction_status'] = initcap(paid_transactions['transaction_status'])
    paid_transactions['transaction_type']   = initcap(paid_transactions['transaction_type'])
    user_ref_statuses['description']        = initcap(user_ref_statuses['description'])
    lead_log['source_category']             = initcap(lead_log['source_category'])
    lead_log['current_status']              = initcap(lead_log['current_status'])

    # Deduplicate user_logs (keep latest record per user_id)
    before = len(user_logs)
    user_logs_clean = (
        user_logs.sort_values('id', ascending=False)
                 .drop_duplicates(subset='user_id', keep='first')
                 .reset_index(drop=True)
    )
    log(f'Deduplicated user_logs: {before} → {len(user_logs_clean)} rows', level=1)

    tables['user_logs_clean'] = user_logs_clean
    return tables


# =============================================================================
#  STEP 4  —  DATA PROCESSING & JOINS
# =============================================================================
def get_source_category(row, lead_map):
    """
    Determine referral_source_category:
      User Sign Up      → Online
      Draft Transaction → Offline
      Lead              → leads.source_category (joined by referee_id → lead_id)
    """
    src = str(row['referral_source']).strip()

    if src == 'User Sign Up':
        return 'Online'
    if src == 'Draft Transaction':
        return 'Offline'
    if src == 'Lead':
        return lead_map.get(row.get('referee_id'), 'Unknown')
    return 'Unknown'


def process_data(tables):
    """Join all tables and compute derived fields."""
    log('STEP 4: Data processing & joins')

    user_referrals    = tables['user_referrals']
    paid_transactions = tables['paid_transactions']
    user_ref_logs     = tables['user_referral_logs']
    user_logs_clean   = tables['user_logs_clean']
    lead_log          = tables['lead_log']
    referral_rewards  = tables['referral_rewards']
    user_ref_statuses = tables['user_referral_statuses']

    # 4a. Source category logic
    lead_map = dict(zip(lead_log['lead_id'], lead_log['source_category']))
    user_referrals['referral_source_category'] = user_referrals.apply(
        lambda r: get_source_category(r, lead_map), axis=1
    )

    # 4b. Map status ID → description
    status_map = dict(zip(user_ref_statuses['id'], user_ref_statuses['description']))
    user_referrals['referral_status'] = user_referrals['user_referral_status_id'].map(status_map)

    # 4c. Map reward ID → number of days
    reward_map = dict(zip(referral_rewards['id'], referral_rewards['reward_value_num']))
    user_referrals['num_reward_days'] = user_referrals['referral_reward_id'].map(reward_map)

    # 4d. Join paid transactions
    df = user_referrals.merge(
        paid_transactions[[
            'transaction_id', 'transaction_status', 'transaction_at',
            'transaction_location', 'transaction_type'
        ]],
        on='transaction_id',
        how='left'
    )

    # 4e. Join referrer info (user_logs)
    referrer_info = user_logs_clean[[
        'user_id', 'name', 'phone_number', 'homeclub',
        'membership_expired_date', 'is_deleted'
    ]].rename(columns={
        'user_id':                 'referrer_id',
        'name':                    'referrer_name',
        'phone_number':            'referrer_phone_number',
        'homeclub':                'referrer_homeclub',
        'membership_expired_date': 'referrer_membership_expired',
        'is_deleted':              'referrer_is_deleted',
    })
    df = df.merge(referrer_info, on='referrer_id', how='left')

    # 4f. Join latest reward log per referral
    reward_log = (
        user_ref_logs.sort_values('created_at')
                     .groupby('user_referral_id')
                     .agg(
                         reward_granted_at=('created_at', 'last'),
                         is_reward_granted=('is_reward_granted', 'last'),
                     )
                     .reset_index()
                     .rename(columns={'user_referral_id': 'referral_id'})
    )
    df = df.merge(reward_log, on='referral_id', how='left')

    # 4g. Convert all timestamps to Asia/Jakarta local time
    df['referral_at']       = to_local_time(df['referral_at'])
    df['updated_at']        = to_local_time(df['updated_at'])
    df['transaction_at']    = to_local_time(df['transaction_at'])
    df['reward_granted_at'] = to_local_time(df['reward_granted_at'])

    # Handle nulls in flags
    df['referrer_is_deleted'] = df['referrer_is_deleted'].fillna(True)
    df['is_reward_granted']   = df['is_reward_granted'].fillna(False)

    log(f'Final merged DataFrame: {len(df)} rows', level=1)
    return df


# =============================================================================
#  STEP 5  —  FRAUD DETECTION (BUSINESS LOGIC)
# =============================================================================
def is_referral_valid(row):
    """
    Apply business logic rules to detect potential fraud.
    Returns True (valid) or False (invalid / potential fraud).

    Valid Conditions:
      V1: Status = Berhasil + reward > 0 + has PAID NEW transaction in same month
          + referrer active + reward granted
      V2: Status = Menunggu / Tidak Berhasil + no reward assigned

    Invalid Conditions (fraud flags):
      I1: Reward > 0 but status is not Berhasil
      I2: Reward > 0 but no transaction ID
      I3: No reward but has PAID transaction after referral
      I4: Status is Berhasil but reward is null/0
      I5: Transaction occurred BEFORE referral creation
    """
    # Extract values
    status      = str(row.get('referral_status', '')).strip()
    reward_days = row.get('num_reward_days')
    tx_id       = row.get('transaction_id')
    tx_status   = str(row.get('transaction_status', '')).strip()
    tx_type     = str(row.get('transaction_type', '')).strip()
    tx_at       = row.get('transaction_at')
    ref_at      = row.get('referral_at')
    expired     = row.get('referrer_membership_expired')

    has_reward   = pd.notna(reward_days) and reward_days > 0
    has_tx       = pd.notna(tx_id)
    is_deleted   = bool(row.get('referrer_is_deleted', True))
    reward_given = bool(row.get('is_reward_granted', False))
    is_expired   = pd.isnull(expired) or (
        pd.notna(expired) and pd.Timestamp(expired).tz_localize(None) < pd.Timestamp.now()
    )

    # ── INVALID CHECKS (run first to flag fraud) ──
    if has_reward and status != 'Berhasil':
        return False                                                   # I1
    if has_reward and not has_tx:
        return False                                                   # I2
    if not has_reward and has_tx and tx_status == 'Paid':
        if pd.notna(tx_at) and pd.notna(ref_at) and tx_at > ref_at:
            return False                                               # I3
    if status == 'Berhasil' and not has_reward:
        return False                                                   # I4
    if has_tx and pd.notna(tx_at) and pd.notna(ref_at) and tx_at < ref_at:
        return False                                                   # I5

    # ── VALID CHECKS ──
    if status == 'Berhasil':
        return all([
            has_reward,
            has_tx,
            tx_status == 'Paid',
            tx_type == 'New',
            pd.notna(tx_at) and pd.notna(ref_at) and tx_at > ref_at,
            pd.notna(tx_at) and pd.notna(ref_at)
                and tx_at.month == ref_at.month
                and tx_at.year == ref_at.year,
            not is_expired,
            not is_deleted,
            reward_given,
        ])                                                             # V1

    if status in ('Menunggu', 'Tidak Berhasil') and not has_reward:
        return True                                                    # V2

    return False


def apply_business_logic(df):
    """Add the is_business_logic_valid flag to every row."""
    log('STEP 5: Fraud detection (business logic)')

    df['is_business_logic_valid'] = df.apply(is_referral_valid, axis=1)

    valid   = int(df['is_business_logic_valid'].sum())
    invalid = len(df) - valid

    log(f'Valid referrals:      {valid}',   level=1)
    log(f'Invalid (flagged):    {invalid}', level=1)
    log(f'Total:                {len(df)}', level=1)
    return df


# =============================================================================
#  STEP 6  —  GENERATE OUTPUT REPORT
# =============================================================================
def generate_report(df):
    """Build the final 46-row report with required columns."""
    log('STEP 6: Generating output report')

    df = df.reset_index(drop=True)
    df['referral_details_id'] = df.index + 101

    report = pd.DataFrame({
        'referral_details_id':      df['referral_details_id'],
        'referral_id':              df['referral_id'],
        'referral_source':          df['referral_source'],
        'referral_source_category': df['referral_source_category'],
        'referral_at':              format_datetime(df['referral_at']),
        'referrer_id':              df['referrer_id'],
        'referrer_name':            df['referrer_name'],
        'referrer_phone_number':    df['referrer_phone_number'],
        'referrer_homeclub':        df['referrer_homeclub'],
        'referee_id':               df['referee_id'],
        'referee_name':             df['referee_name'],
        'referee_phone':            df['referee_phone'],
        'referral_status':          df['referral_status'],
        'num_reward_days':          df['num_reward_days'].astype('Int64'),
        'transaction_id':           df['transaction_id'],
        'transaction_status':       df['transaction_status'],
        'transaction_at':           format_datetime(df['transaction_at']),
        'transaction_location':     df['transaction_location'],
        'transaction_type':         df['transaction_type'],
        'updated_at':               format_datetime(df['updated_at']),
        'reward_granted_at':        format_datetime(df['reward_granted_at']),
        'is_business_logic_valid':  df['is_business_logic_valid'],
    })

    output_path = f'{OUTPUT_DIR}/referral_report.csv'
    report.to_csv(output_path, index=False)

    log(f'Report saved → {output_path}',  level=1)
    log(f'Rows in report: {len(report)}', level=1)
    return report


# =============================================================================
#  MAIN
# =============================================================================
def main():
    print('\n' + '=' * 70)
    print('  GLYNAC WMS — REFERRAL ANALYTICS PIPELINE')
    print('  Author: Arun K  |  Springer Capital Take-Home Test')
    print('=' * 70)

    tables = load_data()
    profile_tables(tables)
    tables = clean_data(tables)
    df     = process_data(tables)
    df     = apply_business_logic(df)
    generate_report(df)

    log('Pipeline complete!')


if __name__ == '__main__':
    main()
