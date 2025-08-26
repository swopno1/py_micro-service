import yfinance as yf
import time

# Wait for 5 seconds to avoid rate limiting
time.sleep(5)

# Search for Reliance Industries
results = yf.Ticker("RELIANCE.NS")
print(results.info)
