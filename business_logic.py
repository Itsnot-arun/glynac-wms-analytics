def is_valid_referral(row):
    try:
        # VALID CONDITIONS
        valid_case_1 = (
            row['reward_value'] > 0 and
            row['referral_status'] == "Berhasil" and
            pd.notnull(row['transaction_id']) and
            row['transaction_status'] == "PAID" and
            row['transaction_type'] == "NEW" and
            row['transaction_at'] > row['referral_at'] and
            row['transaction_at'].month == row['referral_at'].month and
            row['membership_expired_date'] > row['referral_at'] and
            row['is_deleted'] == False and
            row['is_reward_granted'] == True
        )

        valid_case_2 = (
            row['referral_status'] in ["Menunggu", "Tidak Berhasil"] and
            pd.isnull(row['reward_value'])
        )

        if valid_case_1 or valid_case_2:
            return True

        # INVALID CONDITIONS
        if row['reward_value'] > 0 and row['referral_status'] != "Berhasil":
            return False

        if row['reward_value'] > 0 and pd.isnull(row['transaction_id']):
            return False

        if pd.isnull(row['reward_value']) and pd.notnull(row['transaction_id']) and row['transaction_status'] == "PAID":
            return False

        if row['referral_status'] == "Berhasil" and (pd.isnull(row['reward_value']) or row['reward_value'] == 0):
            return False

        if row['transaction_at'] < row['referral_at']:
            return False

        return False

    except:
        return False
