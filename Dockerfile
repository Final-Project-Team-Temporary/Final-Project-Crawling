FROM python:3.13.3-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --upgrade pip \
 && pip install -r requirements.txt

COPY . .

CMD ["bash"]
