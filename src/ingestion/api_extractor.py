import yfinance as yf
import pandas as pd
from datetime import datetime, timezone
from mongo_loader import MongoRawLoader

class YahooBatchExtractor:
    def __init__(self):
        self.loader = MongoRawLoader()

    def get_symbols_from_csv(self, csv_file_path):
        
        print(f"Reading symbols from {csv_file_path}...")
        df = pd.read_csv(csv_file_path)
        
        symbol_col = 'symbol' if 'symbol' in df.columns else df.columns[0]
        symbols = df[symbol_col].dropna().unique().tolist()
        
        print(f"Found {len(symbols)} unique symbols in the file.")
        return symbols

    def fetch_and_store_stock(self, symbol: str, period="1d"):
        
        yahoo_symbol = f"{symbol}.CA" if not symbol.endswith(".CA") else symbol
        print(f"Fetching data for {yahoo_symbol}...")
        
        try:
            stock = yf.Ticker(yahoo_symbol)
            df = stock.history(period=period)
            
            if df.empty:
                print(f"[WARNING] No data found for symbol: {yahoo_symbol}")
                return False

            df.reset_index(inplace=True)
            df['Date'] = df['Date'].dt.strftime('%Y-%m-%d')
            
            records = df.to_dict(orient="records")
            session_date = datetime.now(timezone.utc).strftime('%Y-%m-%d')

            self.loader.load_api_response(
                source="yahoo_finance_live_batch",
                symbol=yahoo_symbol,
                session_date=session_date,
                raw_json_data=records,
                mode="incremental",
                run_id="csv_driven_extraction_001"
            )
            
            print(f"[SUCCESS] Data for {yahoo_symbol} stored successfully!")
            return True

        except Exception as e:
            print(f"[ERROR] Failed to fetch or store data for {yahoo_symbol}: {e}")
            return False

    def run_pipeline_for_csv(self, csv_file_path, period="1d"):
        
        symbols = self.get_symbols_from_csv(csv_file_path)
        
        print(f"Starting extraction for {len(symbols)} stocks...")
        for sym in symbols:
            self.fetch_and_store_stock(sym, period=period)
        
        self.loader.close()
        print("Batch extraction for all CSV symbols completed!")

if __name__ == "__main__":
    
    extractor = YahooBatchExtractor()
    
    csv_path = "data/historical/ALL_divadj.csv"
    
    extractor.run_pipeline_for_csv(csv_path, period="1d")