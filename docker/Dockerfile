FROM mysql:8-debian

RUN apt update && \
    apt install libpq-dev libcurl4-openssl-dev libssl-dev default-libmysqlclient-dev gcc make libjpeg-dev curl git -y \
    && apt install python3-pip -y \
    && apt-get autoremove -y \
    && apt-get clean -y \
    && rm -rf /var/lib/apt/lists/*

RUN pip install -U pip && pip install poetry

WORKDIR /app

COPY pyproject.toml poetry.lock /app/

RUN poetry config virtualenvs.create false \
    && cd /app \
    && poetry install --no-dev --no-interaction --no-ansi \
    && poetry install

COPY . .

# CMD ["/bin/sh", "-c", "make", "run"]
