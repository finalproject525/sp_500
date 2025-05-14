import pandas as pd
import requests
import os
import time
from datetime import datetime

def get_sp500_symbols(url="https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"):
    df = pd.read_html(url, header=0)[0]
    symbols = df['Symbol'].tolist()
    symbols = [s.replace('.', '-') for s in symbols]  # Yahoo format
    return symbols

def get_intraday_data(symbol, interval='60m', range_period='730d'):
    url = f'https://query1.finance.yahoo.com/v8/finance/chart/{symbol}'
    params = {
        'range': range_period,
        'interval': interval,
        'includePrePost': 'false',
        'events': 'div,splits'
    }
    headers = {
        'User-Agent': 'Mozilla/5.0'
    }

    try:
        response = requests.get(url, headers=headers, params=params, timeout=10)
        response.raise_for_status()
        data = response.json()

        result = data['chart']['result'][0]
        timestamps = result['timestamp']
        indicators = result['indicators']['quote'][0]

        df = pd.DataFrame(indicators)
        df['timestamp'] = pd.to_datetime(timestamps, unit='s')
        df.set_index('timestamp', inplace=True)

        return df

    except Exception as e:
        print(f"❌ Erreur pour {symbol} : {e}")
        return None

def load_downloaded_tickers(filepath="data/downloaded.txt"):
    if not os.path.exists(filepath):
        return set()
    with open(filepath, 'r') as f:
        return set(line.strip() for line in f if line.strip())

def save_downloaded_ticker(ticker, filepath="data/downloaded.txt"):
    with open(filepath, 'a') as f:
        f.write(ticker + "\n")

def main():
    output_dir = "data/sp500"
    os.makedirs(output_dir, exist_ok=True)

    downloaded_file = "data/downloaded.txt"
    downloaded = load_downloaded_tickers(downloaded_file)

    symbols = get_sp500_symbols()
    print(f"Nombre total d'actions : {len(symbols)}")

    for i, symbol in enumerate(symbols, start=1):
        if symbol in downloaded:
            print(f"[{i}/{len(symbols)}] ⏭️ Déjà téléchargé : {symbol}")
            continue

        print(f"[{i}/{len(symbols)}] ⬇️ Téléchargement de {symbol}...")
        df = get_intraday_data(symbol)

        if df is not None and not df.empty:
            df.to_csv(f"{output_dir}/{symbol}.csv")
            save_downloaded_ticker(symbol, downloaded_file)
            print(f"✔️ Sauvegardé : {symbol}.csv")
        else:
            print(f"⚠️ Aucune donnée pour {symbol}")

        time.sleep(5)  # Pause entre les requêtes

    print("✅ Fin du téléchargement.")

if __name__ == "__main__":
    main()
