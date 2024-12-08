FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Create logs directory
RUN mkdir -p /app/logs

COPY . .

# Add the app directory to PYTHONPATH
ENV PYTHONPATH="/app:${PYTHONPATH}"

# Set the working directory to the Scrapy project
WORKDIR /app/jobs_project

CMD ["bash"]