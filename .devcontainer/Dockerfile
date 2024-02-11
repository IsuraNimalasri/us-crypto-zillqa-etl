FROM python:3.10

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# Install system dependencies required for Poetry
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    && rm -rf /var/lib/apt/lists/*

# We are using the recommended installer script, but specifying a fixed version for reproducibility
ENV POETRY_VERSION=1.1.13
RUN curl -sSL https://install.python-poetry.org | python3 - --version ${POETRY_VERSION}
# Set the PATH to include Poetry
ENV PATH="/root/.local/bin:$PATH"

CMD ["/bin/sh", "-c", "bash"]
#docker-compose exec my_python sh 
