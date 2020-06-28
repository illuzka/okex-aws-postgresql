FROM python:3
WORKDIR src/app
COPY reqs.txt ./
RUN pip install --no-cache-dir -r reqs.txt
COPY ./ ./
CMD ["python", "data-receiver.py"]
