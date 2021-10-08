FROM python:3.7

ADD crypto_pub.py .
ADD secretkey.py .
ADD proj-data-ed8c87e40cec.json .

RUN pip install requests google-cloud google-cloud-pubsub

CMD [ "python", "./crypto_pub.py" ]