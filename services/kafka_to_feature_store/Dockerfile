FROM python:3.11.2-slim-buster as runtime

# install gcc and python3-dev to compile Python packages
RUN apt-get update && \
    apt-get install -y gcc python3-dev

# add /app/src to the PYTHONPATH
ENV PYTHONPATH="${PYTHONPATH}:/app/src"

# stream output to console
ENV PYTHONUNBUFFERED=1

# install poetry inside the container
RUN pip install poetry==1.8.3

WORKDIR /app

# copy the pyproject.toml and poetry.lock files into the container
COPY pyproject.toml poetry.lock /app

# install Python dependencies from the pyproject.toml file
RUN poetry install --without dev && rm -rf /tmp/poetry_cache

# copy all the source code into the container
COPY ./src /app/src

CMD ["poetry", "run", "python", "src/main.py"]
