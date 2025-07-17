from fastapi import FastAPI, Query
import pandas as pd
import math
import os
from nsepython import nse_optionchain_scrapper
from nsepython import derivative_history
from typing import Optional
import requests
import urllib.request
import random

app = FastAPI()

username = os.getenv("PROXY_USERNAME")
password = os.getenv("PROXY_PASSWORD")
proxy_url = f"http://customer-{username}:{password}@pr.oxylabs.io:7777"
proxies = {
    "http": proxy_url,
    "https": proxy_url,
}
# Globally patch requests
session = requests.Session()
session.proxies.update(proxies)
import nsepython
nsepython.requests = session

# Consumer Price Index
@app.get("/cpi")
async def get_cpi(region: str = Query(...), data: str = Query(...), year: Optional[int] = Query(None)):
    filename = f"{region.lower()}.csv"
    filepath = os.path.join("cpi", data, filename)

    if os.path.exists(os.path.join("cpi", data)):
        if not os.path.exists(filepath):
            return {"error": "404"}
    else:
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
    
# Indian Options Chain
@app.get("/options_chain")
async def options_chain(symbol: str = Query(...)):
    
    username = os.getenv("PROXY_USERNAME")
    password = os.getenv("PROXY_PASSWORD")
    entry = ('http://customer-%s:%s@pr.oxylabs.io:7777' %
        (username, password))
    query = urllib.request.ProxyHandler({
        'http': entry,
        'https': entry,
    })
    execute = urllib.request.build_opener(query)
    
    try:
        data = nse_optionchain_scrapper(symbol)
        return data
    except Exception as e:
        return {"error": str(e)}
        
@app.get("/historical")
async def historical(symbol: str = Query(...), start_date: str = Query(...), end_date: str = Query(...), expiry_date: str = Query(...), strikePrice: int = Query(...), optionType: str = Query(...)):

    username = os.getenv("PROXY_USERNAME")
    password = os.getenv("PROXY_PASSWORD")
    entry = ('http://customer-%s:%s@pr.oxylabs.io:7777' %
        (username, password))
    query = urllib.request.ProxyHandler({
        'http': entry,
        'https': entry,
    })
    execute = urllib.request.build_opener(query)
    
    try:
        df = derivative_history(symbol, start_date, end_date, "options", expiry_date, strikePrice, optionType)
        return df.to_dict(orient="records")
    except Exception as e:
        return {"error": str(e)}