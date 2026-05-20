# Official Python Image, "slim" version to keep the download small.
FROM python:3.12.3-slim

# Java required for Apache Spark
RUN apt-get update && \
    apt-get install -y default-jre && \
    apt-get clean

# All subsequent commands will be run from inside this /app folder.
WORKDIR /app

# Docker caches layers; if the code changes but not your requirements,
# Docker doesn't have to redownload all the libraries.
COPY requirements.txt .

# The --no-cache-dir flag is a Docker best practice to keep the image size down.
RUN pip install --no-cache-dir -r requirements.txt

# Now we copy the rest of the files into the /app directory.
COPY . .

CMD ["python", "simulation.py"]