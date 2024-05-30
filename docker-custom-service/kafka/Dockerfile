FROM python:3.11.2

WORKDIR /trip/

COPY ./trip-data-generation .
COPY ./requirements.txt .

RUN pip3 install --no-cache-dir -r requirements.txt
