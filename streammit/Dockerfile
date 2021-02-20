FROM python:3.8-alpine
WORKDIR /code
COPY requirements.txt .
RUN pip install -r requirements.txt
COPY ./ .
CMD [ "python", "-u",  "./streammit.py" ]
