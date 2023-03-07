# To add a new cell, type '# %%'
# To add a new markdown cell, type '# %% [markdown]'
# %%
import pandas as pd

# %%
df = pd.read_excel("data/2020/10/hcpc/anweb.xlsx", 
                    usecols="A,D:AV", 
                    parse_dates=True, 
                    na_values=["NA", "0"], 
                    header=10,).dropna(how='all')


# %%
def convert_dates_to_text(float_input: float):
    text = float_input.astype(str).apply(lambda s: s.replace('.0',''))
    return text.apply(lambda t: t[:4] + '-' + t[4:6] + '-' + t[6:8])

# %%
df[['ADD DT', 'ACT EFF DT', 'TERM DT']] = df[['ADD DT', 'ACT EFF DT', 'TERM DT']].apply(lambda s: convert_dates_to_text(s))

# %%
df['TERM DT'].value_counts()


# %%



