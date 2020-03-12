FROM python:3.8

WORKDIR /usr/src/app

RUN mkdir /data

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

CMD [ "python", "./download.py", "/data" ]
