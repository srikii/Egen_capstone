import os
from os import environ
from time import sleep
import time
from concurrent import futures
import requests
import logging
from google.cloud import pubsub_v1
from google.cloud.pubsub_v1 import PublisherClient
from google.cloud.pubsub_v1.publisher.futures import Future
#from api import url, headers, querystring
from secretkey import key

#importing service key for authentication of project 
credential_path = r"proj-data-ed8c87e40cec.json" 
os.environ['GOOGLE_APPLICATION_CREDENTIALS']=credential_path

class CrypToPubsub:
    def __init__(self):
        '''initialize variables'''

        self.project_id="proj-data"
        self.topic_id="cryptodata"
        self.Publisher_client=PublisherClient()
        self.topic_path=self.Publisher_client.topic_path(self.project_id,self.topic_id)
        self.publish_futute=[]

    def get_bitcoin_data(self) -> str:
        
        response=requests.get(f"https://api.nomics.com/v1/currencies/ticker?key={key}&interval=1h&convert=USD&per-page=50&page=1")
        time.sleep(1)#sleep 1 seconds after getting requests data

        if response.status_code==200:
            return response.text
        else:
            raise Exception(f"failed to fetch crypto data {response.status_code}: {response.text} ")
    
    def get_callback(self, publish_futute, data: str) ->callable:
        def callback(publish_futute):
            try:
                logging.info(publish_futute.result(timeout=60))
            except futures.TimeoutError:
                logging.error(f"publishing {data} timed out")
            
        return callback


    def publish_to_topic(self, message:str) ->None:

        publish_futute=self.Publisher_client.publish(self.topic_path, message.encode("utf-8"))

        publish_futute.add_done_callback(self.get_callback(publish_futute,message))
        self.publish_futute.append(publish_futute)

        futures.wait(self.publish_futute, return_when=futures.ALL_COMPLETED)

        print(f"Published messages to {self.topic_path}.")
        logging.info(f"published message to {self.topic_path}")




if __name__ == "__main__":


    svc=CrypToPubsub()

    for i in range(50):
        message=svc.get_bitcoin_data()
        svc.publish_to_topic(message)
        sleep(300)
        
