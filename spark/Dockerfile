FROM apache/spark:4.0.0

USER root

# 1. Install prerequisites (software-properties-common allows adding repos)
RUN apt-get update && apt-get install -y \
    software-properties-common \
    curl \
    procps \
    openjdk-17-jre-headless \
    && apt-get clean

# 2. Add the Deadsnakes PPA to get Python 3.12
# The -y flag is crucial to prevent the build from hanging
RUN add-apt-repository ppa:deadsnakes/ppa -y

# 3. Now install Python 3.12
# Note: python3.12-distutils is replaced by python3.12-dev/venv in 3.12+
RUN apt-get update && apt-get install -y \
    python3.12 \
    python3.12-dev \
    python3.12-venv \
    && apt-get clean

# 4. Force 'python' and 'python3' to point to Python 3.12
RUN update-alternatives --install /usr/bin/python3 python3 /usr/bin/python3.12 1 \
    && update-alternatives --install /usr/bin/python python /usr/bin/python3.12 1

# 5. Install Pip for Python 3.12
RUN curl -sS https://bootstrap.pypa.io/get-pip.py | python3.12

# 6. Set Environment Variables
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PIP_BREAK_SYSTEM_PACKAGES=1

# 7. Install Python Libraries
RUN pip install --no-cache-dir \
    pyspark==4.0.0 \
    snowflake-connector-python \
    snowflake-sqlalchemy \
    python-dotenv

USER spark