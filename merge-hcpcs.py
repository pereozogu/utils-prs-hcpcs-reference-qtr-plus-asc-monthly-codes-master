import pandas as pd
from datetime import date
import numpy as np

# ASC approved HCPCS codes and payment rates -- month/every other month
excel3 = 'data/2020/07/section-508/asc_addenda.xlsx'

# reading specific sheets and will combine them into a second file
# drop the top section of the excel sheet
# comments in the excel sheet are irrelevant to the data needed below
df4 = pd.read_excel(
    io=excel3, usecols="A, C", sheet_name='CY 2020 Jul ASC AA',
    header=3, nrows=3977,
).dropna(how='all')

df5 = pd.read_excel(
    io=excel3, usecols="A, C", sheet_name='CY 2020 Jul ASC BB',
    header=2, nrows=1823,
).dropna(how='all')

# separating this from its' siblings as the end result
# is 3 columns with staggered data instead of 2 columns.
df6 = pd.read_excel(
    io=excel3, usecols="A, B", sheet_name='CY 2020 Jul ASC EE',
    header=3, nrows=2064,
).dropna(how='all')

# Define a helper function to fix truncated zeroes using one parameter: the hcpcs code passed in

def fix_trunc_zeroes(val):
    # Use if statements to check for 3 chareacters in the incoming values
    if len(str(val)) == 3:
        # if true append 2 zeroes in the front
        return "00" + str(val)
     # Use else if statements to check for 4 chareacters in the incoming values
    elif len(str(val)) == 4:
        # if true append one zero in the front
        return "0" + str(val)
    # Otherwise ...
    else:
        # Return the value itself
        return str(val)

# Run helper function the hcpcs fields to apply
df4['HCPCS Code'] = df4['HCPCS Code'].apply(lambda x: fix_trunc_zeroes(x))
df5['HCPCS Code'] = df5['HCPCS Code'].apply(lambda x: fix_trunc_zeroes(x))
df6['HCPCS Code'] = df6['HCPCS Code'].apply(lambda x: fix_trunc_zeroes(x))

# df4['HCPCS Code'] = df4['HCPCS Code'].astype('str')
# df4['HCPCS Code'] = df4['HCPCS Code'].str.strip()
# df4.loc[df4['HCPCS Code'].str.len() == 5]

# df5['HCPCS Code'] = df5['HCPCS Code'].astype('str')
# df5['HCPCS Code'] = df5['HCPCS Code'].str.strip()
# df5.loc[df5['HCPCS Code'].str.len() == 5]

# df6['HCPCS Code'] = df6['HCPCS Code'].astype('str')
# df6['HCPCS Code'] = df6['HCPCS Code'].str.strip()
# df6.loc[df6['HCPCS Code'].str.len() == 5]

# Rename columns
df4.columns = ["HCPCS_CODE", "HCPCS_DESCRIPTION"]
df5.columns = ["HCPCS_CODE", "HCPCS_DESCRIPTION"]
df6.columns = ["HCPCS_CODE", "HCPCS_DESCRIPTION"]

# adding in other columns
df4 = df4.assign(
    HCPCS_ASC_EXCLUDED="N",
    HCPCS_ASC_EXCLUDED_DT=np.nan,
    HCPCS_GROUP="Y",
    HCPCS_GROUP_BEGIN_DT=date(date.today().year, 1, 1),
    HCPCS_GROUP_END_DT=date(date.today().year, 12, 31),
    ADDED_TO_FILE_DATE=date.today(),
    LAST_EDIT_DATE=date.today(),
    USER_ID="Raw asc data from CMS")

df5 = df5.assign(
    HCPCS_ASC_EXCLUDED="N",
    HCPCS_ASC_EXCLUDED_DT=np.nan,
    HCPCS_GROUP="Y",
    HCPCS_GROUP_BEGIN_DT=date(date.today().year, 1, 1),
    HCPCS_GROUP_END_DT=date(date.today().year, 12, 31),
    ADDED_TO_FILE_DATE=date.today(),
    LAST_EDIT_DATE=date.today(),
    USER_ID="Raw asc data from CMS")

# codes to be excluded for 2020 August_2020
df6 = df6.assign(
    HCPCS_ASC_EXCLUDED="Y",
    HCPCS_ASC_EXCLUDED_DT=date.today(),
    HCPCS_GROUP="Y",
    HCPCS_GROUP_BEGIN_DT=date(date.today().year, 1, 1),
    HCPCS_GROUP_END_DT=date(date.today().year, 12, 31),
    ADDED_TO_FILE_DATE=date.today(),
    LAST_EDIT_DATE=date.today(),
    USER_ID="Raw asc data excluded for 2020 payment from CMS")

join_all = pd.concat(
    [df4, df5, df6],
    sort=True
).drop_duplicates()

# Reorder columns
join_all = join_all[["HCPCS_CODE",
                     "HCPCS_DESCRIPTION",
                     "HCPCS_ASC_EXCLUDED",
                     "HCPCS_ASC_EXCLUDED_DT",
                     "HCPCS_GROUP",
                     "HCPCS_GROUP_BEGIN_DT",
                     "HCPCS_GROUP_END_DT",
                     "ADDED_TO_FILE_DATE",
                     "LAST_EDIT_DATE",
                     "USER_ID"]]

join_all.to_csv("output/asc_hcpcs_codes.csv")
join_all.to_parquet("output/asc_hcpcs_codes.parquet")