FROM python:3.11-buster AS builder

# install gcc and python3-dev to compile Python packages
RUN apt-get update && \
    apt-get install -y gcc python3-dev

# install poetry inside the container
RUN pip install poetry==1.8.3 && \
    poetry config virtualenvs.in-project true

WORKDIR /app

# copy the pyproject.toml and poetry.lock files into the container
COPY pyproject.toml poetry.lock /app

# install Python dependencies from the pyproject.toml file
RUN poetry install --without dev && rm -rf /tmp/poetry_cache

FROM python:3.11.2-slim-buster AS runtime

ENV PYTHONUNBUFFERED=1 \
    PATH="/app/.venv/bin:$PATH"

# install poetry inside the container
RUN pip install poetry==1.8.3 && \
    poetry config virtualenvs.in-project true

COPY --from=builder /app/.venv /app/.venv

# copy all the source code into the container
COPY ./poetry.lock ./pyproject.toml /app
COPY ./src /app/src

WORKDIR /app

CMD ["poetry", "run", "python", "-m", "src.main"]
