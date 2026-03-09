# Use a lightweight Python image
FROM python:3.10-slim

# Set working directory
WORKDIR /app

# Install system dependencies for Snowflake and Spark
RUN apt-get update && apt-get install -y \
    build-essential \
    libgomp1 \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements and install
COPY requirements.txt .
RUN pip install -r requirements.txt

# Copy the rest of the code
COPY . .

# Environment variable for Ollama (Important for Docker-to-Host communication)
ENV OLLAMA_BASE_URL=http://host.docker.internal:11434

# Run the agent
CMD ["streamlit", "run", "agent.py", "--server.port=8501", "--server.address=0.0.0.0"]