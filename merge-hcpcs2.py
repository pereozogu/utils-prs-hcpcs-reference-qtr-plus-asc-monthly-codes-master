import pandas as pd
from datetime import date
import datetime

# quarterly HCPCS alpha-numerics
excel1 = 'data/2020/10/hcpc/anweb.xlsx'

df = pd.read_excel(io=excel1,
                   usecols="A,D:AV",
                   parse_dates=True,
                   na_values=["NA", "0"],
                   header=10,).dropna(how='all')

df['HCPC'] = df['HCPC'].astype('str')
df['HCPC'] = df['HCPC'].str.strip()
df.loc[df['HCPC'].str.len() == 5]


def convert_dates_to_text(float_input: float):
    text = float_input.astype(str).apply(lambda s: s.replace('.0', ''))
    return text.apply(lambda t: t[:4] + '-' + t[4:6] + '-' + t[6:8])


def int_to_string(val):
    return str(val)


df[['ASC_DT', 'OPPS_DT', 'ADD DT', 'ACT EFF DT', 'TERM DT']] = df[['ASC_DT', 'OPPS_DT', 'ADD DT', 'ACT EFF DT', 'TERM DT']].apply(lambda s: convert_dates_to_text(s))

# Rename columns
df.columns = ["HCPCS_CODE", "LONG_DESCRIPTION", "SHORT_DESCRIPTION", "PRICE1", "PRICE2", "PRICE3", "PRICE4",
              "MULT_PI", "CIM1", "CIM2", "CIM3", "MCM1", "MCM2", "MCM3", "STATUTE", "LABCERT1", "LABCERT2",
              "LABCERT3", "LABCERT4", "LABCERT5", "LABCERT6", "LABCERT7", "LABCERT8", "XREF1", "XREF2", "XREF3",
              "XREF4", "XREF5", "COV", "ASC_GRP", "ASC_DT", "OPPS", "OPPS_PI", "OPPS_DT", "PROCNOTE", "BETOS",
              "TOS1", "TOS2", "TOS3", "TOS4", "TOS5", "ANEST_BU", "ADD_DT", "ACT_EFF_DT", "TERM_DT", "ACTION_CD"]

# adding in other columns
df = df.assign(
    CLNDR_HCPCS_YR_NUM=int_to_string(date.today().year))

join_all = pd.concat(
    [df],
    sort=True
).drop_duplicates()

# Reorder columns
join_all = join_all[["HCPCS_CODE", "CLNDR_HCPCS_YR_NUM", "LONG_DESCRIPTION", "SHORT_DESCRIPTION", "PRICE1", "PRICE2",
                     "PRICE3", "PRICE4", "MULT_PI", "CIM1", "CIM2", "CIM3", "MCM1", "MCM2", "MCM3", "STATUTE", "LABCERT1",
                     "LABCERT2", "LABCERT3", "LABCERT4", "LABCERT5", "LABCERT6", "LABCERT7", "LABCERT8", "XREF1", "XREF2",
                     "XREF3", "XREF4", "XREF5", "COV", "ASC_GRP", "ASC_DT", "OPPS", "OPPS_PI", "OPPS_DT", "PROCNOTE", "BETOS",
                     "TOS1", "TOS2", "TOS3", "TOS4", "TOS5", "ANEST_BU", "ADD_DT", "ACT_EFF_DT", "TERM_DT", "ACTION_CD"]]

join_all.to_csv("output/anweb_hcpcs_codes.csv")
join_all.to_parquet("output/anweb_hcpcs_codes.parquet")
