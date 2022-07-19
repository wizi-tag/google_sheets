FROM python:3.8

WORKDIR /script

COPY requirements.txt .

RUN pip install -r requirements.txt

COPY . .

CMD ["python", "./script.py"]
