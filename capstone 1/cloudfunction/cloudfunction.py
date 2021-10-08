import base64
import logging
from base64 import b64decode
from pandas import DataFrame
from json import loads
import json
from google.cloud.storage import Client

class LoadToStorage:
     def __init__(self,event,context):
          self.event=event
          self.context=context
          self.bucket_name="proj_data_bucket"
     
     def get_message_data(self) -> str:
          "extract data from pubsub"

          logging.info(
               f"this function was triggered by message {self.context.event_id} published at {self.context.timestamp}"
               f"to {self.context.resource['name']}")

          if "data" in self.event:
               pubsub_message=b64decode(self.event["data"]).decode("utf-8")
               logging.info(pubsub_message)
               return pubsub_message
          else:
               logging.error("incorrect format")
               return " "

     def transform_payload_to_df(self, message:str)-> DataFrame:
          "transform json to dataframe"

          try:
               data=json.loads(message)
               df=DataFrame(data)
               if not df.empty:
                    logging.info(f"created Dataframe with  {df.shape[0]} rows and {df.shape[1]} columns")
               else:
                    logging.warning(f"created empty dataframe") 
               return df
          except Exception as e:
               logging.error(f"encountered error creating dataframe {str(e)}")
               raise
     
     def upload_to_bucket(self, df:DataFrame, file_name: str="payload")-> None:
          "upload DF to GCS bucket in csv"


          storage_client=Client()
          bucket=storage_client.bucket(self.bucket_name)
          blob=bucket.blob(f"{file_name}.csv")

          blob.upload_from_string(data=df.to_csv(index=False), content_type="text/csv")

          logging.info(f"file uploaded to {self.bucket_name}")

def process(event,context):

     root=logging.getLogger()
     root.setLevel(logging.INFO)

     svc=LoadToStorage(event, context)
     message=svc.get_message_data()
     upload_df=svc.transform_payload_to_df(message)

     #append timestamp to end of file name
     payload_timestamp=upload_df["price_timestamp"].unique().tolist()[0]
     svc.upload_to_bucket(upload_df,"crypto data"+str(payload_timestamp))


