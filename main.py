from fastapi import FastAPI, Query
import pandas as pd
import math
import os
from nsepython import nse_optionchain_scrapper, derivative_history
from typing import Optional
import requests

app = FastAPI()

def maybe_enable_proxy():
    """Temporarily enable proxy settings for nsepython if USE_PROXY=true."""
    if os.getenv("USE_PROXY", "false").lower() == "true":
        username = os.getenv("PROXY_USERNAME")
        password = os.getenv("PROXY_PASSWORD")
        proxy_url = f"http://customer-{username}:{password}@pr.oxylabs.io:7777"
        os.environ["HTTP_PROXY"] = proxy_url
        os.environ["HTTPS_PROXY"] = proxy_url
    else:
        os.environ.pop("HTTP_PROXY", None)
        os.environ.pop("HTTPS_PROXY", None)

@app.get("/options_chain")
async def options_chain(symbol: str = Query(...)):
    try:
        maybe_enable_proxy()
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
        maybe_enable_proxy()
        df = derivative_history(symbol, start_date, end_date, "options", expiry_date, strikePrice, optionType)
        return df.to_dict(orient="records")
    except Exception as e:
        return {"error": str(e)}
