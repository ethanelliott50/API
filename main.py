from fastapi import FastAPI, Query
import pandas as pd
import math
import os
from nsepython import nse_optionchain_scrapper, derivative_history
from typing import Optional
import requests
from urllib.parse import quote

app = FastAPI()

def configure_proxy_globally_if_needed():
    """If USE_PROXY=true, patch requests globally to use proxy."""
    if os.getenv("USE_PROXY", "false").lower() == "true":
        username = os.getenv("PROXY_USERNAME")
        password = quote(os.getenv("PROXY_PASSWORD", ""), safe="")
        proxy_url = f"http://customer-{username}:{password}@pr.oxylabs.io:7777"
        proxies = {
            "http": proxy_url,
            "https": proxy_url,
        }
        # Globally patch requests
        session = requests.Session()
        session.proxies.update(proxies)
        import nsepython
        nsepython.requests = session  # Monkey-patch requests used inside nsepython
        print("✅ Proxy is enabled and injected into nsepython")

# Call this once at startup
configure_proxy_globally_if_needed()

@app.get("/cpi")
async def get_cpi(region: str = Query(...), data: str = Query(...), year: Optional[int] = Query(None)):
    filename = f"{region.lower()}.csv"
    filepath = os.path.join("cpi", data, filename)

    if not os.path.exists(filepath):
        return {"error": "404"}

    df = pd.read_csv(filepath)

    if year is not None:
        if year not in df["Year"].values:
            return {"error": "404"}
        df = df[df["Year"] == year]

    result = {}
    for _, row in df.iterrows():
        year_key = str(int(row["Year"]))
        result[year_key] = {
            col: row[col]
            for col in df.columns
            if col != "Year" and not pd.isna(row[col]) and not (isinstance(row[col], float) and math.isnan(row[col]))
        }

    return {
        "region": region,
        "data": data,
        "cpi": result
    }

@app.get("/options_chain")
async def options_chain(symbol: str = Query(...)):
    try:
        data = nse_optionchain_scrapper(symbol)
        return data
    except Exception as e:
        return {"error": str(e)}

@app.get("/historical")
async def historical(
    symbol: str = Query(...),
    start_date: str = Query(...),
    end_date: str = Query(...),
    expiry_date: str = Query(...),
    strikePrice: int = Query(...),
    optionType: str = Query(...)
):
    try:
        df = derivative_history(symbol, start_date, end_date, "options", expiry_date, strikePrice, optionType)
        return df.to_dict(orient="records")
    except Exception as e:
        return {"error": str(e)}
