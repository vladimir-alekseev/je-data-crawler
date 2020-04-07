FROM python:3.7-slim
COPY requirements.txt /tmp
RUN pip install --no-cache-dir -r /tmp/requirements.txt

COPY data_crawler/ data_crawler/

ENV PYTHONPATH .
ENTRYPOINT ["python3", "data_crawler/__main__.py"]
CMD ["-c", "config.ini"]