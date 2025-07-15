from fastapi import FastAPI, Query
import pandas as pd
import math
import os
from nsepython import nse_optionchain_scrapper
from typing import Optional

app = FastAPI()

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
async def get_options_chain(symbol: str = Query(...)):
    try:
        data = nse_optionchain_scrapper(symbol)
        return data
    except Exception as e:
        return {"error": str(e)}
