import logging
import azure.functions as func
import json 
import time
from azure.eventhub import EventHubProducerClient, EventData
from azure.identity import DefaultAzureCredential 

app = func.FunctionApp()

@app.timer_trigger(schedule="*/30 * * * * *", arg_name="myTimer", run_on_startup=False,
              use_monitor=False) 
def tradesfunction(myTimer: func.TimerRequest) -> None:
    if myTimer.past_due:
        logging.info('The timer is past due!')

    logging.info('Python timer trigger function executed.')

    # Event Hub Configuration 
    EVENT_HUB_NAMESPACE = "tradestreamingnamespace.servicebus.windows.net"
    EVENT_HUB_NAME = "tradestreamingnamespace"

    # Uses Managed Identity of Function App 
    credential = DefaultAzureCredential()

    # Initialse the Event Hub producer 
    producer = EventHubProducerClient(
        fully_qualified_namespace=EVENT_HUB_NAMESPACE,
        eventhub_name=EVENT_HUB_NAME,
        credential=credential
    )

    # Function to send events to Event Hub 
    def send_event(event):
        event_data_batch = producer.create_batch()
        event_data_batch.add(EventData(json.dumps(event)))
        producer.send_batch(event_data_batch)
    
    # Example Streaming Data
    def generate_trade_data():
        return {
            "tradeId": "T" + str(int(time.time() * 1000)),
            "symbol": "AAPL",
            "price": round(170 + (5 * (0.5 - time.time() % 1)), 2),  # Simulate price variation
            "quantity": 100,
            "tradeType": "BUY",
            "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "traderId": "T12345",
            "exchange": "NASDAQ"x
        }
    
    # Main program
    def fetch_trade_data():
        trade_data = generate_trade_data()
        send_event(trade_data)
        

    # Calling the Main Program
    fetch_trade_data()
