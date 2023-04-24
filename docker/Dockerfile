FROM mysql:8.0


RUN curl -O https://bootstrap.pypa.io/get-pip.py \
    && python3 get-pip.py \
    && rm -rf /var/lib/apt/lists/*

RUN pip3 install --upgrade pip && pip3 install poetry

WORKDIR /app

COPY pyproject.toml poetry.lock /app/

RUN poetry config virtualenvs.create false \
    && cd app \
    && poetry install --no-dev --no-interaction --no-ansi \
    && poetry install

COPY . .

# CMD ["/bin/sh", "-c", "make", "run"]
