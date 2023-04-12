from decouple import config

class Settings:
    DATABASE_URL_PG = str(config("DATABASE_URL_PG"))
    DATABASE_URL_MSQL = str(config("DATABASE_URL_MSQL"))


settings = Settings()
