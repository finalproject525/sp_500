import pandas as pd

def get_sp500_symbol(url="https://en.wikipedia.org/wiki/List_of_S%26P_500_companies#S&P_500_component_stocks"):

    df = pd.read_html(url, header=0)[0]
    #df.to_csv("data/constituents.csv", index=False)
    #df.to_json("data/sp500_tickers.json", orient="records", indent=2)
    return df
