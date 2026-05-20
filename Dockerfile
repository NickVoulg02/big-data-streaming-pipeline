# This stage installs all the heavy tools needed to compile the Python packages.
FROM python:3.12.3-slim AS builder

WORKDIR /app

# We install everything into a specific folder (/app/dependencies) so we can easily copy it later
COPY requirements.txt .
RUN pip install --no-cache-dir --prefix=/app/dependencies -r requirements.txt

# This stage is totally fresh. It throws away the builder and only keeps the final files.
FROM python:3.12.3-slim

WORKDIR /app

# We need Java to run Spark
RUN apt-get update && \
    apt-get install -y default-jre && \
    apt-get clean

# We copy ONLY the compiled libraries from the 'builder' stage
COPY --from=builder /app/dependencies /usr/local

# Copy your Python scripts and data
COPY . .

CMD ["python", "simulation.py"]