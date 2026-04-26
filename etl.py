"""
Glynac WMS Referral Analytics — ETL Pipeline
Transforms raw CSV exports into analytics-ready JSON
"""

import pandas as pd
import json
import os

DATA_DIR = os.path.join(os.path.dirname(__file__), '..', 'data')
OUTPUT_DIR = os.path.join(os.path.dirname(__file__), '..', 'public')

os.makedirs(OUTPUT_DIR, exist_ok=True)


def load_data():
    return {
        'lead_log': pd.read_csv(f'{DATA_DIR}/lead_log.csv'),
        'paid_transactions': pd.read_csv(f'{DATA_DIR}/paid_transactions.csv'),
        'referral_rewards': pd.read_csv(f'{DATA_DIR}/referral_rewards.csv'),
        'user_logs': pd.read_csv(f'{DATA_DIR}/user_logs.csv'),
        'user_referral_logs': pd.read_csv(f'{DATA_DIR}/user_referral_logs.csv'),
        'user_referral_statuses': pd.read_csv(f'{DATA_DIR}/user_referral_statuses.csv'),
        'user_referrals': pd.read_csv(f'{DATA_DIR}/user_referrals.csv'),
    }


def transform(data):
    user_referrals = data['user_referrals'].copy()
    user_ref_logs = data['user_referral_logs'].copy()
    user_logs = data['user_logs'].copy()
    paid_tx = data['paid_transactions'].copy()
    lead_log = data['lead_log'].copy()
    ref_rewards = data['referral_rewards'].copy()

    # Status mapping
    status_map = {1: 'Pending', 2: 'Success', 3: 'Failed'}
    user_referrals['status_label'] = user_referrals['user_referral_status_id'].map(status_map)

    # Referral funnel
    referral_funnel = user_referrals['status_label'].value_counts().to_dict()

    # Referral sources
    referral_sources = user_referrals['referral_source'].value_counts().to_dict()

    # Reward grant rate
    total_logs = len(user_ref_logs)
    rewarded = int(user_ref_logs['is_reward_granted'].sum())
    reward_rate = round(rewarded / total_logs * 100, 1) if total_logs > 0 else 0

    # User activity
    active_users = int((user_logs['is_deleted'] == False).sum())
    deleted_users = int((user_logs['is_deleted'] == True).sum())

    # Transactions
    tx_by_location = paid_tx['transaction_location'].value_counts().to_dict()
    tx_by_type = paid_tx['transaction_type'].value_counts().to_dict()

    # Leads
    lead_status = lead_log['current_status'].value_counts().to_dict()
    lead_source = lead_log['source_category'].value_counts().to_dict()

    # Referrals over time
    user_referrals['referral_at'] = pd.to_datetime(user_referrals['referral_at'], utc=True)
    referrals_monthly = user_referrals.groupby(
        user_referrals['referral_at'].dt.to_period('M')
    ).size()
    referrals_monthly = {str(k): int(v) for k, v in referrals_monthly.items()}

    # Homeclub distribution (unique users)
    homeclub_dist = user_logs.drop_duplicates('user_id')['homeclub'].value_counts().to_dict()

    # KPIs
    kpis = {
        'total_referrals': len(user_referrals),
        'successful_referrals': referral_funnel.get('Success', 0),
        'pending_referrals': referral_funnel.get('Pending', 0),
        'failed_referrals': referral_funnel.get('Failed', 0),
        'conversion_rate': round(referral_funnel.get('Success', 0) / len(user_referrals) * 100, 1),
        'total_transactions': len(paid_tx),
        'reward_granted_rate': reward_rate,
        'total_rewards_granted': rewarded,
        'active_users': active_users,
        'deleted_users': deleted_users,
        'total_leads': len(lead_log),
    }

    return {
        'kpis': kpis,
        'referral_funnel': referral_funnel,
        'referral_sources': referral_sources,
        'tx_by_location': tx_by_location,
        'tx_by_type': tx_by_type,
        'lead_status': lead_status,
        'lead_source': lead_source,
        'referrals_monthly': referrals_monthly,
        'homeclub_dist': homeclub_dist,
    }


def load_analytics():
    data = load_data()
    analytics = transform(data)
    output_path = os.path.join(OUTPUT_DIR, 'analytics.json')
    with open(output_path, 'w') as f:
        json.dump(analytics, f, indent=2)
    print(f"✅ Analytics written to {output_path}")
    return analytics


if __name__ == '__main__':
    result = load_analytics()
    print(json.dumps(result['kpis'], indent=2))
