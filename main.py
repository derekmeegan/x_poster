import functions_framework
from google.oauth2 import service_account
from googleapiclient.discovery import build
import pandas as pd
import os
import requests
import tweepy
import json

def get_sheet_values(sheet_name: str, sheet_id: str = '1RtcWspwxO0xsFg-vSJ9CabKbDhaamoIxtvk3dzvDZxc'):
    credentials = service_account.Credentials.from_service_account_file(
        'credentials.json', scopes=['https://www.googleapis.com/auth/spreadsheets'])
    service = build('sheets', 'v4', credentials=credentials)
    sheet = service.spreadsheets()
    result = (
        sheet.values()
        .get(spreadsheetId=sheet_id, range = sheet_name)
        .execute()
    )
    return result.get("values", [])


def convert_holdings_response_to_df(values):
    return pd.DataFrame([x for x in values[2:-1] if x], columns = values[1])

def get_stocks_from_sheet():
    return (
        convert_holdings_response_to_df(get_sheet_values('stocks'))
        .assign(
            quantity = lambda df_: df_.quantity.str.replace(',', '').astype(float)
        )
    )

def get_stock_profile_info(stock_symbols):
    url = f"https://financialmodelingprep.com/api/v3/profile/{','.join(stock_symbols)}"
    FINANCE_API_KEY =os.environ.get('FINANCE_API_KEY')
    params = {
        'apikey': FINANCE_API_KEY
    }
    
    # Make the HTTP GET request
    response = requests.get(url, params=params)
    return pd.DataFrame(json.loads(response.text))

@functions_framework.cloud_event
def post(request):
    todays_results = (
        get_stocks_from_sheet()
        .groupby('asset').sum()
        [['quantity']]
        .reset_index()
        .assign(
            asset = lambda df_: df_.asset.str.upper()
        )
        .pipe(
            lambda df_: (
                df_
                .merge(
                    df_.asset.pipe(get_stock_profile_info),
                    left_on = 'asset',
                    right_on = 'symbol'
                )
            )
        )
        .assign(
            percent_change = lambda df_: (df_.changes / (df_.price + (df_.changes * -1)) * 100).round(2),
            money_change = lambda df_: df_.changes * df_.quantity
        )
    )
    top_percent_gainer = todays_results.sort_values('percent_change', ascending = False).iloc[0]
    top_monetary_gainer = todays_results.sort_values('money_change', ascending = False).iloc[0]
    worst_percent_gainer = todays_results.sort_values('percent_change').iloc[0]
    worst_monetary_gainer = todays_results.sort_values('money_change').iloc[0]

    t_p_change = top_percent_gainer['percent_change']
    t_m_change = top_monetary_gainer['money_change']

    w_p_change = worst_percent_gainer['percent_change']
    w_m_change = worst_monetary_gainer['money_change']

    total_port_change = (
        todays_results
        .assign(
            value = lambda df_: df_.quantity * df_.price, 
            portfolio_proportion = lambda df_: df_.value /df_.value.sum(),
            weighted_change = lambda df_: df_.percent_change * df_.portfolio_proportion
        )
        .weighted_change.sum() 
    ).round(2)

    X_CONSUMER_SECRET=os.environ.get('X_CONSUMER_SECRET')
    X_CONSUMER_KEY=os.environ.get('X_CONSUMER_KEY')
    X_ACCESS_TOKEN=os.environ.get('X_ACCESS_TOKEN')
    X_ACCESS_TOKEN_SECRET=os.environ.get('X_ACCESS_TOKEN_SECRET')

    client = tweepy.Client(
        access_token=X_ACCESS_TOKEN, 
        access_token_secret=X_ACCESS_TOKEN_SECRET, 
        consumer_key=X_CONSUMER_KEY, 
        consumer_secret=X_CONSUMER_SECRET
    )
    #Top Monetary Gainer: {top_monetary_gainer['asset']} ({'+' + '$' + str(t_m_change) if t_m_change > 0 else '$' + str(t_m_change)}) {'🟢' if t_m_change> 0 else '🔴'}
    #Worst Monetary Loss: {worst_monetary_gainer['asset']} ({'+' + '$' + str(w_m_change) if w_m_change > 0 else '$' + str(w_m_change)}) {'🟢' if w_m_change> 0 else '🔴'}
    text = f'''Trading session is closed, let's take a look at today's results:

Overall {'Gain' if total_port_change > 0 else 'Loss'} {'🟢' if total_port_change> 0 else '🔴'} : {total_port_change}%

Top Gainer {'🟢' if t_p_change> 0 else '🔴'} : ${top_percent_gainer['asset']} ({'+' + str(t_p_change) if t_p_change > 0 else t_p_change}%)
    
Top Loser {'🟢' if w_p_change> 0 else '🔴'} : ${worst_percent_gainer['asset']} ({'+' + str(w_p_change) if w_p_change > 0 else w_p_change}%)
    ''' 
    client.create_tweet(text=text)

    return 'success'
    