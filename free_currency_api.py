# This is a sample Python script.

# Press ⌃R to execute it or replace it with your code.
# Press Double ⇧ to search everywhere for classes, files, tool windows, actions, and settings.
import freecurrencyapi
import requests
import json
from dataclasses import dataclass
from google.cloud import firestore
from datetime import datetime, timedelta
from dotenv import load_dotenv
import pytz
import os

load_dotenv()  # Load environment variables from .env
freecurrencyapi_key = os.getenv("FREE_CURRENCY_API_KEY")
sgt_timezone = pytz.timezone('Asia/Singapore')

# freecurrencyapi_key = 'fca_live_3B7A9qDncgTIXqW1ab8Fw7EJ6M3VA8i4nJv2QLed'
base_currency = 'SGD'
default_na_value = -1
decimal_precision = 3
headers = {"Firebase": "no"}
db = firestore.Client()


@dataclass
class CurrencyMetadata:
    name: str
    symbol: str
    char_symbol: str
    tag: str
    wise_url: str
    collection_name: str
    notification_topic: str


@dataclass
class RateData:
    base_curr: str
    target_curr: str
    target_curr_symbol: str
    today_rate: float
    change_since_yesterday: float
    last_week_avg: float
    week_over_week_change: float

    def __str__(self):
        change_sign = '🟢+' if self.change_since_yesterday > 0 else '🔸+' if self.change_since_yesterday == 0.0 else '🔻'
        today_rate_str = f"{self.target_curr_symbol} {round_off(self.today_rate)} ({change_sign}{self.change_since_yesterday})"
        last_week_avg_str = f"{self.target_curr_symbol} {self.last_week_avg}"
        return (
            f"Today's rate        | {today_rate_str}\n"
            f"Last week's avg  | {last_week_avg_str}\n")


currency_metadata = {
    'INR': CurrencyMetadata(name='Indian Rupee', symbol='₹', char_symbol='INR', tag='india',
                            wise_url='https://wise.com/gb/currency-converter/sgd-to-inr-rate',
                            collection_name='sgd_inr_exchange_data', notification_topic='dbarr_inr_updates'),
    'MYR': CurrencyMetadata(name='Malaysian Ringgit', symbol='RM', char_symbol='RM', tag='malaysia',
                            wise_url='https://wise.com/gb/currency-converter/sgd-to-myr-rate',
                            collection_name='sgd_myr_exchange_data', notification_topic='dbarr_myr_updates')
}


def get_latest_exchange_rate(currency):
    print(f'Getting exchange rate for {currency} from FreeCurencyAPI')
    client = freecurrencyapi.Client(freecurrencyapi_key)
    result = client.latest(base_currency=base_currency, currencies=[currency])
    print("Returned the following data from free currency api : " + json.dumps(result))
    return result


def write_to_store(currency, exchange_rate_data):
    try:
        print("Writing exchange data to firestore")
        collection_name = currency_metadata[currency].collection_name
        collection_ref = db.collection(collection_name)
        today_date = '{:%Y-%m-%d}'.format(datetime.now(sgt_timezone))
        today_rate = exchange_rate_data['data']
        print("Today's date " + today_date)
        collection_ref.document(today_date).set(today_rate)
        print(f"Successfully persisted to firestore.")
    except Exception as e:
        print(f"An unexpected error occurred while writing to firestore : {e}")
    finally:
        print("Write to store method finished.")


def push_notify(data):
    curr_metadata = currency_metadata[data.target_curr]
    push_url = "https://ntfy.sh/"
    payload_data = {
        "topic": curr_metadata.notification_topic,
        "markdown": True,
        "title": f"SGD-{data.target_curr} exchange rate",
        "tags": [curr_metadata.tag],
        "actions": [{"action": "view", "label": "View current rate",
                     "url": curr_metadata.wise_url}],
        "message": data.__str__()
    }
    print(payload_data)
    requests.post(push_url, data=json.dumps(payload_data), headers=headers)


def calculate_stats(currency, exchange_rate_data):
    today_rate = round_off(exchange_rate_data['data'][currency])
    today = datetime.now(sgt_timezone)
    yesterday = today - timedelta(days=1)
    week_before = today - timedelta(days=7)
    last_week_avg = calculate_average(currency, week_before, yesterday)
    change_since_yesterday = calculate_change(currency, yesterday, today_rate)
    data = RateData(base_curr=base_currency, target_curr=currency,
                    target_curr_symbol=currency_metadata[currency].symbol,
                    today_rate=exchange_rate_data['data'][currency], change_since_yesterday=change_since_yesterday,
                    last_week_avg=last_week_avg, week_over_week_change=default_na_value)
    return data


def calculate_average(currency, start, end):
    print("Calculating average - ")
    start_id = '{:%Y-%m-%d}'.format(start)
    end_id = '{:%Y-%m-%d}'.format(end)
    print(f"start = {start_id} and end = {end_id} ")
    collection_name = currency_metadata[currency].collection_name
    docs = db.collection(collection_name) \
        .order_by("__name__") \
        .start_at({"__name__": start_id}) \
        .end_at({"__name__": end_id}) \
        .stream()
    data = {}
    for doc in docs:
        print(f"Document ID: {doc.id} => Data: {doc.to_dict()}")
        data[doc.id] = doc.get(currency)

    print(f"data = {data}")
    average = -1 if len(data) == 0 else sum(data.values()) / len(data)
    print(f"avg  = {average}")
    return round_off(average)


def calculate_change(currency, yesterday, today_rate):
    yesterday_date = '{:%Y-%m-%d}'.format(yesterday)
    collection_name = currency_metadata[currency].collection_name
    doc_ref = db.collection(collection_name).document(yesterday_date)
    doc = doc_ref.get()
    if doc.exists:
        print(f"Document data: {doc.to_dict()}")
        yesterday_rate = round_off(doc.get(currency))
        print(f"Yesterday rate = {yesterday_rate} and today rate = {today_rate}")
        change_since_yesterday = round_off(today_rate - yesterday_rate)
        print(f"Change since yesterday = {change_since_yesterday}")
        return change_since_yesterday
    else:
        print("No such document!")
    return default_na_value


def round_off(value):
    return round(value, decimal_precision)


def process(currency, store_data, send_notification):
    exchange_rate_data = get_latest_exchange_rate(currency)
    data = calculate_stats(currency, exchange_rate_data)
    if send_notification:
        push_notify(data)
    if store_data:
        write_to_store(currency, exchange_rate_data)


if __name__ == '__main__':
    for currency in currency_metadata.keys():
        process(currency=currency, store_data=False, send_notification=True)
    print("Process completed.")