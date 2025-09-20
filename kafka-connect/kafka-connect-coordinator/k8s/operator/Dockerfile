FROM python:3.11-slim

# Install system dependencies
RUN apt-get update && apt-get install -y \
    docker.io \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies
COPY requirements.txt /tmp/
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# Create operator user
RUN useradd --create-home --shell /bin/bash operator

# Copy operator code
COPY operator.py /app/
WORKDIR /app

# Set ownership and permissions
RUN chown -R operator:operator /app
USER operator

# Run the operator
CMD ["python3", "operator.py"]