from fastapi import FastAPI, Query
import pandas as pd
import math
import os
from typing import Optional
import requests
import urllib.request
import random

app = FastAPI()

headers = {
            "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
            "accept-language": "en-US,en;q=0.9,en-IN;q=0.8,en-GB;q=0.7",
            "cache-control": "max-age=0",
            "priority": "u=0, i",
            "sec-ch-ua": '"Microsoft Edge";v="129", "Not=A?Brand";v="8", "Chromium";v="129"',
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": '"Windows"',
            "sec-fetch-dest": "document",
            "sec-fetch-mode": "navigate",
            "sec-fetch-site": "none",
            "sec-fetch-user": "?1",
            "upgrade-insecure-requests": "1",
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36 Edg/129.0.0.0"
        }
        
indices = ['NIFTY','FINNIFTY','BANKNIFTY']

def nsefetch(payload):
    try:
        s = requests.Session()

        # Optional proxy setup
        if os.getenv("USE_PROXY", "false").lower() == "true":
            username = os.getenv("PROXY_USERNAME")
            password = os.getenv("PROXY_PASSWORD")
            proxy = f"http://customer-{username}:{password}@pr.oxylabs.io:7777"
            s.proxies.update({
                "http": proxy,
                "https": proxy,
            })

        # NSE headers
        s.get("https://www.nseindia.com", headers=headers, timeout=10)
        s.get("https://www.nseindia.com/option-chain", headers=headers, timeout=10)
        output = s.get(payload, headers=headers, timeout=10).json()
    except ValueError:
        output = {}
    except Exception as e:
        output = {"error": str(e)}
    return output
        
def nsesymbolpurify(symbol):
    symbol = symbol.replace('&','%26') #URL Parse for Stocks Like M&M Finance
    return symbol
    
def nse_optionchain_scrapper(symbol):
    symbol = nsesymbolpurify(symbol)
    if any(x in symbol for x in indices):
        payload = nsefetch('https://www.nseindia.com/api/option-chain-indices?symbol='+symbol)
    else:
        payload = nsefetch('https://www.nseindia.com/api/option-chain-equities?symbol='+symbol)
    return payload
    
def derivative_history_virgin(symbol,start_date,end_date,instrumentType,expiry_date,strikePrice="",optionType=""):

    instrumentType = instrumentType.lower()

    if(instrumentType=="options"):
        instrumentType="OPTSTK"
        if("NIFTY" in symbol): instrumentType="OPTIDX"
        
    if(instrumentType=="futures"):
        instrumentType="FUTSTK"
        if("NIFTY" in symbol): instrumentType="FUTIDX"
        

    #if(((instrumentType=="OPTIDX")or (instrumentType=="OPTSTK")) and (expiry_date!="")):
    if(strikePrice!=""):
        strikePrice = "%.2f" % strikePrice
        strikePrice = str(strikePrice)

    nsefetch_url = "https://www.nseindia.com/api/historical/fo/derivatives?&from="+str(start_date)+"&to="+str(end_date)+"&optionType="+optionType+"&strikePrice="+strikePrice+"&expiryDate="+expiry_date+"&instrumentType="+instrumentType+"&symbol="+symbol+""
    payload = nsefetch(nsefetch_url)
    logging.info(nsefetch_url)
    logging.info(payload)
    return pd.DataFrame.from_records(payload["data"])

def derivative_history(symbol,start_date,end_date,instrumentType,expiry_date,strikePrice="",optionType=""):
    #We are getting the input in text. So it is being converted to Datetime object from String.
    start_date = datetime.datetime.strptime(start_date, "%d-%m-%Y")
    end_date = datetime.datetime.strptime(end_date, "%d-%m-%Y")
    logging.info("Starting Date: "+str(start_date))
    logging.info("Ending Date: "+str(end_date))

    #We are calculating the difference between the days
    diff = end_date-start_date
    logging.info("Total Number of Days: "+str(diff.days))
    logging.info("Total FOR Loops in the program: "+str(int(diff.days/40)))
    logging.info("Remainder Loop: " + str(diff.days-(int(diff.days/40)*40)))


    total=pd.DataFrame()
    for i in range (0,int(diff.days/40)):

        temp_date = (start_date+datetime.timedelta(days=(40))).strftime("%d-%m-%Y")
        start_date = datetime.datetime.strftime(start_date, "%d-%m-%Y")

        logging.info("Loop = "+str(i))
        logging.info("====")
        logging.info("Starting Date: "+str(start_date))
        logging.info("Ending Date: "+str(temp_date))
        logging.info("====")

        #total=total.append(derivative_history_virgin(symbol,start_date,temp_date,instrumentType,expiry_date,strikePrice,optionType))
        #total=total.concat([total, derivative_history_virgin(symbol,start_date,temp_date,instrumentType,expiry_date,strikePrice,optionType)])
        total = pd.concat([total, derivative_history_virgin(symbol, start_date, temp_date, instrumentType, expiry_date, strikePrice, optionType)])


        logging.info("Length of the Table: "+ str(len(total)))

        #Preparation for the next loop
        start_date = datetime.datetime.strptime(temp_date, "%d-%m-%Y")


    start_date = datetime.datetime.strftime(start_date, "%d-%m-%Y")
    end_date = datetime.datetime.strftime(end_date, "%d-%m-%Y")

    logging.info("End Loop")
    logging.info("====")
    logging.info("Starting Date: "+str(start_date))
    logging.info("Ending Date: "+str(end_date))
    logging.info("====")

    #total=total.append(derivative_history_virgin(symbol,start_date,end_date,instrumentType,expiry_date,strikePrice,optionType))
    #total = total.concat([total, derivative_history_virgin(symbol,start_date,end_date,instrumentType,expiry_date,strikePrice,optionType)])
    total = pd.concat([total, derivative_history_virgin(symbol, start_date, end_date, instrumentType, expiry_date, strikePrice, optionType)])



    logging.info("Finale")
    logging.info("Length of the Total Dataset: "+ str(len(total)))
    payload = total.iloc[::-1].reset_index(drop=True)
    return payload

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
    
    try:
        data = nse_optionchain_scrapper(symbol)
        return data
    except Exception as e:
        return {"error": str(e)}
        
@app.get("/historical")
async def historical(symbol: str = Query(...), start_date: str = Query(...), end_date: str = Query(...), expiry_date: str = Query(...), strikePrice: int = Query(...), optionType: str = Query(...)):
    
    try:
        df = derivative_history(symbol, start_date, end_date, "options", expiry_date, strikePrice, optionType)
        return df.to_dict(orient="records")
    except Exception as e:
        return {"error": str(e)}