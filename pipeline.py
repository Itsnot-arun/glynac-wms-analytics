"""
Glynac WMS — Referral Analytics Pipeline
Springer Capital Take-Home Test: Data Engineering Intern
Author: Arun K
"""

import pandas as pd
import numpy as np
import re
import os
import pytz
from datetime import datetime

DATA_DIR = os.path.join(os.path.dirname(__file__), 'data')
OUTPUT_DIR = os.path.join(os.path.dirname(__file__), 'output')
os.makedirs(OUTPUT_DIR, exist_ok=True)

# load all the source tables
print("Loading data...")

lead_log = pd.read_csv(f'{DATA_DIR}/lead_log.csv')
paid_transactions = pd.read_csv(f'{DATA_DIR}/paid_transactions.csv')
referral_rewards = pd.read_csv(f'{DATA_DIR}/referral_rewards.csv')
user_logs = pd.read_csv(f'{DATA_DIR}/user_logs.csv')
user_ref_logs = pd.read_csv(f'{DATA_DIR}/user_referral_logs.csv')
user_ref_statuses = pd.read_csv(f'{DATA_DIR}/user_referral_statuses.csv')
user_referrals = pd.read_csv(f'{DATA_DIR}/user_referrals.csv')

print(f"lead_log: {len(lead_log)} rows")
print(f"paid_transactions: {len(paid_transactions)} rows")
print(f"referral_rewards: {len(referral_rewards)} rows")
print(f"user_logs: {len(user_logs)} rows")
print(f"user_referral_logs: {len(user_ref_logs)} rows")
print(f"user_referral_statuses: {len(user_ref_statuses)} rows")
print(f"user_referrals: {len(user_referrals)} rows")

# data profiling — null counts and distinct values per column
print("\nProfiling tables...")

def profile_table(df, table_name):
    rows = []
    for col in df.columns:
        rows.append({
            'table_name': table_name,
            'column_name': col,
            'data_type': str(df[col].dtype),
            'null_count': int(df[col].isnull().sum()),
            'null_pct': round(df[col].isnull().mean() * 100, 2),
            'distinct_count': int(df[col].nunique()),
            'sample_value': str(df[col].dropna().iloc[0]) if len(df[col].dropna()) > 0 else 'N/A'
        })
    return pd.DataFrame(rows)

tables = {
    'lead_log': lead_log,
    'paid_transactions': paid_transactions,
    'referral_rewards': referral_rewards,
    'user_logs': user_logs,
    'user_referral_logs': user_ref_logs,
    'user_referral_statuses': user_ref_statuses,
    'user_referrals': user_referrals,
}

profile_dfs = [profile_table(df, name) for name, df in tables.items()]
profiling_report = pd.concat(profile_dfs, ignore_index=True)
profiling_report.to_csv(f'{OUTPUT_DIR}/data_profiling_report.csv', index=False)
print("Profiling report saved to output/data_profiling_report.csv")
print(profiling_report[['table_name', 'column_name', 'null_count', 'distinct_count']].to_string(index=False))

# cleaning
print("\nCleaning data...")

def parse_ts(series):
    return pd.to_datetime(series, utc=True, errors='coerce')

user_referrals['referral_at'] = parse_ts(user_referrals['referral_at'])
user_referrals['updated_at'] = parse_ts(user_referrals['updated_at'])
paid_transactions['transaction_at'] = parse_ts(paid_transactions['transaction_at'])
user_ref_logs['created_at'] = parse_ts(user_ref_logs['created_at'])
user_logs['membership_expired_date'] = pd.to_datetime(user_logs['membership_expired_date'], errors='coerce', utc=True)
lead_log['created_at'] = parse_ts(lead_log['created_at'])

# pull numeric days out of strings like "10 days"
referral_rewards['reward_value_num'] = referral_rewards['reward_value'].str.extract(r'(\d+)').astype(float)

def initcap(series):
    return series.apply(lambda x: x.title() if isinstance(x, str) else x)

user_referrals['referee_name'] = initcap(user_referrals['referee_name'])
user_referrals['referral_source'] = initcap(user_referrals['referral_source'])
paid_transactions['transaction_status'] = initcap(paid_transactions['transaction_status'])
paid_transactions['transaction_type'] = initcap(paid_transactions['transaction_type'])
user_ref_statuses['description'] = initcap(user_ref_statuses['description'])
lead_log['source_category'] = initcap(lead_log['source_category'])
lead_log['current_status'] = initcap(lead_log['current_status'])

# keep only the most recent record per user in user_logs
user_logs_clean = (
    user_logs.sort_values('id', ascending=False)
    .drop_duplicates(subset='user_id', keep='first')
    .reset_index(drop=True)
)
print(f"user_logs after dedup: {len(user_logs_clean)} rows (was {len(user_logs)})")

# processing and joins
print("\nJoining tables...")

# source category:
#   User Sign Up -> Online, Draft Transaction -> Offline, Lead -> from lead_log
def get_source_category(row, lead_map):
    src = str(row['referral_source']).strip()
    if src == 'User Sign Up':
        return 'Online'
    elif src == 'Draft Transaction':
        return 'Offline'
    elif src == 'Lead':
        return lead_map.get(row.get('referee_id'), 'Unknown')
    return 'Unknown'

lead_map = dict(zip(lead_log['lead_id'], lead_log['source_category']))
user_referrals['referral_source_category'] = user_referrals.apply(
    lambda r: get_source_category(r, lead_map), axis=1
)

status_map = dict(zip(user_ref_statuses['id'], user_ref_statuses['description']))
user_referrals['referral_status'] = user_referrals['user_referral_status_id'].map(status_map)

reward_map_value = dict(zip(referral_rewards['id'], referral_rewards['reward_value_num']))
user_referrals['num_reward_days'] = user_referrals['referral_reward_id'].map(reward_map_value)

tx_cols = ['transaction_id', 'transaction_status', 'transaction_at',
           'transaction_location', 'transaction_type']
df = user_referrals.merge(
    paid_transactions[tx_cols],
    on='transaction_id',
    how='left',
    suffixes=('', '_tx')
)

referrer_cols = ['user_id', 'name', 'phone_number', 'homeclub', 'membership_expired_date', 'is_deleted']
df = df.merge(
    user_logs_clean[referrer_cols].rename(columns={
        'user_id': 'referrer_id',
        'name': 'referrer_name',
        'phone_number': 'referrer_phone_number',
        'homeclub': 'referrer_homeclub',
        'membership_expired_date': 'referrer_membership_expired',
        'is_deleted': 'referrer_is_deleted'
    }),
    on='referrer_id',
    how='left'
)

# grab the latest reward log entry per referral
reward_log = (
    user_ref_logs.sort_values('created_at')
    .groupby('user_referral_id')
    .agg(
        reward_granted_at=('created_at', 'last'),
        is_reward_granted=('is_reward_granted', 'last')
    )
    .reset_index()
    .rename(columns={'user_referral_id': 'referral_id'})
)
df = df.merge(reward_log, on='referral_id', how='left')

# convert to Jakarta time (UTC+7)
jkt = pytz.timezone('Asia/Jakarta')

def to_local(series):
    if series.dt.tz is None:
        series = series.dt.tz_localize('UTC')
    return series.dt.tz_convert(jkt).dt.tz_localize(None)

df['referral_at'] = to_local(df['referral_at'])
df['updated_at'] = to_local(df['updated_at'])
df['transaction_at'] = to_local(df['transaction_at'])
df['reward_granted_at'] = to_local(df['reward_granted_at'])

df['referrer_is_deleted'] = df['referrer_is_deleted'].fillna(True)
df['is_reward_granted'] = df['is_reward_granted'].fillna(False)

print(f"Merged DataFrame: {len(df)} rows")

# fraud / business logic validation
print("\nRunning business logic checks...")

def is_valid_referral(row):
    status = str(row.get('referral_status', '')).strip()
    reward_days = row.get('num_reward_days')
    tx_id = row.get('transaction_id')
    tx_status = str(row.get('transaction_status', '')).strip()
    tx_type = str(row.get('transaction_type', '')).strip()
    tx_at = row.get('transaction_at')
    ref_at = row.get('referral_at')

    is_expired = pd.isnull(row.get('referrer_membership_expired')) or (
        pd.notna(row.get('referrer_membership_expired')) and
        pd.Timestamp(row['referrer_membership_expired']).tz_localize(None) < pd.Timestamp.now()
    )
    is_deleted = bool(row.get('referrer_is_deleted', True))
    reward_given = bool(row.get('is_reward_granted', False))
    has_reward = pd.notna(reward_days) and reward_days > 0
    has_tx = pd.notna(tx_id)

    # invalid: reward assigned but status not successful
    if has_reward and status != 'Berhasil':
        return False

    # invalid: reward assigned but no transaction
    if has_reward and not has_tx:
        return False

    # invalid: no reward but there's a paid transaction after referral date
    if not has_reward and has_tx and tx_status == 'Paid':
        if pd.notna(tx_at) and pd.notna(ref_at) and tx_at > ref_at:
            return False

    # invalid: status says successful but no reward
    if status == 'Berhasil' and not has_reward:
        return False

    # invalid: transaction happened before the referral was created
    if has_tx and pd.notna(tx_at) and pd.notna(ref_at):
        if tx_at < ref_at:
            return False

    # valid successful referral — all conditions must hold
    if status == 'Berhasil':
        conditions = [
            has_reward,
            has_tx,
            tx_status == 'Paid',
            tx_type == 'New',
            pd.notna(tx_at) and pd.notna(ref_at) and tx_at > ref_at,
            pd.notna(tx_at) and pd.notna(ref_at) and tx_at.month == ref_at.month and tx_at.year == ref_at.year,
            not is_expired,
            not is_deleted,
            reward_given,
        ]
        return all(conditions)

    # pending or failed with no reward is fine
    if status in ('Menunggu', 'Tidak Berhasil') and not has_reward:
        return True

    return False

df['is_business_logic_valid'] = df.apply(is_valid_referral, axis=1)

valid_count = df['is_business_logic_valid'].sum()
invalid_count = len(df) - valid_count
print(f"Valid: {valid_count}, Invalid: {invalid_count}, Total: {len(df)}")

# build the final report
print("\nGenerating report...")

df = df.reset_index(drop=True)
df['referral_details_id'] = df.index + 101

def fmt_dt(series):
    return series.apply(lambda x: x.strftime('%Y-%m-%d %H:%M:%S') if pd.notna(x) else None)

report = pd.DataFrame({
    'referral_details_id': df['referral_details_id'],
    'referral_id': df['referral_id'],
    'referral_source': df['referral_source'],
    'referral_source_category': df['referral_source_category'],
    'referral_at': fmt_dt(df['referral_at']),
    'referrer_id': df['referrer_id'],
    'referrer_name': df['referrer_name'],
    'referrer_phone_number': df['referrer_phone_number'],
    'referrer_homeclub': df['referrer_homeclub'],
    'referee_id': df['referee_id'],
    'referee_name': df['referee_name'],
    'referee_phone': df['referee_phone'],
    'referral_status': df['referral_status'],
    'num_reward_days': df['num_reward_days'].astype('Int64'),
    'transaction_id': df['transaction_id'],
    'transaction_status': df['transaction_status'],
    'transaction_at': fmt_dt(df['transaction_at']),
    'transaction_location': df['transaction_location'],
    'transaction_type': df['transaction_type'],
    'updated_at': fmt_dt(df['updated_at']),
    'reward_granted_at': fmt_dt(df['reward_granted_at']),
    'is_business_logic_valid': df['is_business_logic_valid'],
})

report.to_csv(f'{OUTPUT_DIR}/referral_report.csv', index=False)
print(f"Report saved: output/referral_report.csv ({len(report)} rows)")
print(report[['referral_id', 'referral_status', 'num_reward_days', 'is_business_logic_valid']].head(10).to_string(index=False))

print("\nDone.")
