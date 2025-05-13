import yfinance as yf
from typing import List, Dict
import pandas as pd

class YahooFinanceClientByTicker:
    def __init__(self, symbol: str):
        self.symbol = symbol.upper()
        self.ticker = yf.Ticker(self.symbol)

    def get_stock_history(self, period: str = '1d', interval: str = '1m') -> pd.DataFrame:
        """
            [1m, 2m, 5m, 15m, 30m, 60m, 90m, 1h, 1d, 5d, 1wk, 1mo, 3mo]
        """
        return self.ticker.history(period=period, interval=interval)
    
class YahooBatchFinanceClient:
    def __init__(self,symbols: List[str], period: str = '1d', interval: str = '1m', batch_size: int = 2):
        self.symbols = list(dict.fromkeys(symbols))
        self.period = period
        self.interval = interval
        self.batch_size = batch_size
        self.data_by_symbol: Dict[str, pd.DataFrame] = {}


    def _chunk_list(self,lst,size):
        for i in range(0,len(lst),size):
            yield lst[i:i+size]  

    def fetch_all(self) -> Dict[str, pd.DataFrame]:
        for batch in self._chunk_list(self.symbols, self.batch_size):
            tickers_str = " ".join(batch)
            data = yf.download(
                tickers=tickers_str,
                period=self.period,
                interval=self.interval,
                group_by="ticker",
                threads=True,
                progress=False
            )


            if isinstance(data.columns, pd.MultiIndex):
                for symbol in batch:
                    try:
                        df = data[symbol].copy()
                        df.dropna(how="all", inplace=True)
                        df["symbol"] = symbol
                        self.data_by_symbol[symbol] = df


                    except KeyError:
                        print(f"⚠️ No data for {symbol}")
            else:
                # Single ticker, data is flat
                symbol = batch[0]
                data["symbol"] = symbol
                self.data_by_symbol[symbol] = data

        return self.data_by_symbol

    def to_dict_records(self) -> List[Dict]:
        records = []
        for df in self.data_by_symbol.values():
            for timestamp, row in df.iterrows():
                record = row.to_dict()
                record["timestamp"] = pd.Timestamp.now().timestamp()  
                record["datetime"] = timestamp.isoformat()  
                records.append(record)
        return records
    
    def get_symbol_data(self, symbol: str) -> pd.DataFrame:
        return self.data_by_symbol.get(symbol.upper())