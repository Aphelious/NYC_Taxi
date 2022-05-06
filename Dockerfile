FROM python:3.9

RUN pip install pandas sqlalchemy

WORKDIR /app
COPY ingest_data.py ingest_data.py

CMD [ "python", "ingest_data.py" ]
