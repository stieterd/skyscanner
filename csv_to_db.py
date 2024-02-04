import pandas as pd
import os
import matplotlib.pyplot as plt
from datetime import datetime
from sqlalchemy import create_engine

DB_NAME = "flights"
engine = create_engine(f"mysql+pymysql://root:Walvis12@107.172.63.184/{DB_NAME}")

# df1 = pd.read_csv("outbound_2024-01-16-12.csv", index_col=False).iloc[: , 1:]
# # df1.to_sql('flight', engine, index=False, if_exists='replace')
# # exit()
# df2 = pd.read_csv("outbound_2024-01-16-13.csv")

# merged_df = pd.merge(df1, df2, on='hash', suffixes=('_df1', '_df2'))

# result_df = merged_df[merged_df['price_df1'] != merged_df['price_df2']]

# print(result_df.to_csv("result_df.csv"))

# outputs = os.listdir(".")
# # print(outputs)
# outbound_files = [file for file in outputs if file.startswith('outbound')]
# return_files = [file for file in outputs if file.startswith('return')]
# # Find the latest outbound file
# latest_outbound = max(outbound_files, key=lambda x: datetime.strptime(x.split('_')[1], '%Y-%m-%d-%H.csv'))
# # Find the latest return file
# latest_return = max(return_files, key=lambda x: datetime.strptime(x.split('_')[1], '%Y-%m-%d-%H.csv'))

# print(latest_outbound)
# print(latest_return)

# print(sorted(outputs, key = lambda d: datetime.strptime(d, '%Y-%m-%d'),  reverse=True))
# print()
# exit()

def filter_price_change(group):
    # Check if there is more than one unique value in the 'price' column
    return group['price'].nunique() > 10


outbound_dfs = []
return_dfs = []

for filename in os.listdir("output_data"):
    f = "output_data/" + os.path.join(filename)
    # checking if it is a file
    if not os.path.isfile(f):
        print(f)
        continue

    if f.endswith('.csv'):
        if 'outbound' in f:
            try:
                df = pd.read_csv(f).dropna(axis=1, how='all').iloc[:, 1:]
                # outbound_dfs.append(pd.read_csv(f).dropna(axis=1, how='all'))
                if 'scrapeDate' in df.columns and 'hash' in df.columns:
                    df.to_sql('flight', engine, index=False, if_exists='append')
            except Exception as e:
                df = pd.read_csv(f).dropna(axis=1, how='all')
                # outbound_dfs.append(pd.read_csv(f).dropna(axis=1, how='all'))
                if 'scrapeDate' in df.columns and 'hash' in df.columns:
                    df = df[df['departureStation'] != ""]
                    df = df[~df['scrapeDate'].isnull()]
                    df.to_sql('flight', engine, index=False, if_exists='append')


        elif 'return' in f:
            try:
                df = pd.read_csv(f).dropna(axis=1, how='all').iloc[:, 1:]
                # return_dfs.append(pd.read_csv(f).dropna(axis=1, how='all'))

                if 'scrapeDate' in df.columns and 'hash' in df.columns:
                    df.to_sql('flight', engine, index=False, if_exists='append')
            except Exception as e:
                df = pd.read_csv(f).dropna(axis=1, how='all')
                # outbound_dfs.append(pd.read_csv(f).dropna(axis=1, how='all'))
                if 'scrapeDate' in df.columns and 'hash' in df.columns:
                    df = df[df['departureStation'] != ""]
                    df = df[~df['scrapeDate'].isnull()]
                    df.to_sql('flight', engine, index=False, if_exists='append')

exit()

# outbound_df = pd.concat(outbound_dfs, ignore_index=True)
# return_df = pd.concat(return_dfs, ignore_index=True)

# outbound_df = outbound_df.iloc[: , 1:].reset_index(drop=True)
# return_df = return_df.iloc[: , 1:].reset_index(drop=True)

# print(outbound_df)
# # outbound_df.to_csv("test.csv", index=False)
# # exit()

# outbound_df = outbound_df.drop('ticketUrl', axis=1)
# return_df = return_df.drop('ticketUrl', axis=1)

# outbound_df.drop_duplicates(subset=None, keep="first", inplace=True)
# return_df.drop_duplicates(subset=None, keep="first", inplace=True)

# outbound_df = outbound_df[~outbound_df['hash'].isnull()]
# return_df = return_df[~return_df['hash'].isnull()]
# outbound_groups = outbound_df.groupby('hash')
# return_df = return_df.groupby('hash')

# # grp = outbound_groups.get_group(outbound_groups.size().idxmax())
# outbound_groups = outbound_groups.filter(filter_price_change)

# for name, grp in outbound_groups.groupby('hash'):
# # grp.to_csv("test.csv", index=False)
#     print(grp['price'])
#     print(grp['scrapeDate'])
#     print(grp['arrivalStation'])
#     print(grp['departureStation'])
# exit()
# print(grp)
# print(grp.columns)
# grp.sort_values('scrapeDate', ascending=True)

# grp = grp.reset_index(drop=True)
# # Set the 'date' column as the index (if not already)
# # grp.set_index('scrapeDate', inplace=True)

# print(grp['price'])
# print(grp[''])

# # Plotting the chart using Pandas plot method
# grp['price'].plot(marker='o', linestyle='-')
# # Adding labels and title
# plt.xlabel('Date')
# plt.ylabel('Price')
# plt.title('Price Chart')

# # Display the chart
# plt.show()