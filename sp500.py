### 1 Create Datasets
# downloading and saving data ready for zipline ingest
# 1.1 Imports

import os
from datetime import date

import pandas as pd

os.chdir(os.path.dirname(os.path.abspath(__file__)))


def create_constituents(df):
    # create Dataframe with current constituents
    ticker_list = []
    for i, row in df.iterrows():
        tmp_val = row['ticker']
        ticker_list.append(tmp_val)

    res_string = ','.join(ticker_list)

    results_df = pd.DataFrame({'date': date.today(),
                               'tickers': [res_string],
                               })

    return results_df


def main():
    # read historical data
    sp500_hist = pd.read_csv('sp_500_historical_components.csv')

    # current companies
    sp_500_url = 'https://en.wikipedia.org/wiki/List_of_S%26P_500_companies'
    sp_500_constituents = pd.read_html(sp_500_url, header=0)[0].rename(columns=str.lower)
    sp_500_constituents['date'] = date.today()
    sp_500_constituents.to_csv('sp500_constituents.csv', index=False)
    sp_500_constituents.drop(['gics sector', 'gics sub-industry',
                              'headquarters location', 'date added',
                              'cik', 'founded', 'security'], axis=1, inplace=True)

    sp_500_constituents.columns = ['ticker', 'date']
    sp_500_constituents.sort_values(by='ticker', ascending=True,inplace=True)

    df = create_constituents(sp_500_constituents)
    final = pd.concat([sp500_hist, df], ignore_index=True)

    # output
    final = final.drop_duplicates(subset=['date', 'tickers'],keep='last')
    final.to_csv('sp_500_historical_components.csv', index=False)
    

if __name__ == '__main__':
    main()
