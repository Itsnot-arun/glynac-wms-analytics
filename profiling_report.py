import pandas as pd
import os

DATA_PATH = "data"

def profile_table(file):
    df = pd.read_csv(file)
    profile = []

    for col in df.columns:
        profile.append({
            "table": os.path.basename(file),
            "column": col,
            "null_count": df[col].isnull().sum(),
            "distinct_count": df[col].nunique()
        })

    return profile

all_profiles = []

for file in os.listdir(DATA_PATH):
    if file.endswith(".csv"):
        all_profiles.extend(profile_table(os.path.join(DATA_PATH, file)))

profile_df = pd.DataFrame(all_profiles)
profile_df.to_csv("profiling_report.csv", index=False)

print("Profiling report generated!")
