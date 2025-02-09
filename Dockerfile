# Use a smaller Python image
FROM python:3.9-slim  


RUN pip install pandas

WORKDIR /app

COPY pipeline.py pipeline.py

ENTRYPOINT ["python", "pipeline.py"]


