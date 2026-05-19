# 1. FROM: This defines the base image. Docker Docs recommend using official images.
# We are using Python 3.12.3 (as used in your project) and the "slim" version to keep the download small.
FROM python:3.12.3-slim

# Install Java (required for Apache Spark)
RUN apt-get update && \
    apt-get install -y default-jre && \
    apt-get clean

# 2. WORKDIR: Docker Docs advise setting a working directory inside the container.
# All subsequent commands will be run from inside this /app folder.
WORKDIR /app

# 3. COPY (Dependencies): We copy the requirements file FIRST.
# Docker caches layers; doing this first means if you change your code but not your requirements,
# Docker doesn't have to redownload all the libraries.
COPY requirements.txt .

# 4. RUN: This executes a command during the build phase. We are installing the Python packages.
# The --no-cache-dir flag is a Docker best practice to keep the image size down.
RUN pip install --no-cache-dir -r requirements.txt

# 5. COPY (Source Code): Now we copy the rest of your project files (like simulation.py) into the /app directory.
COPY . .

# 6. CMD: This tells Docker the default command to run when the container starts.
CMD ["python", "simulation.py"]