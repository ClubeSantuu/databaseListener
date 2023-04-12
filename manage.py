#!/usr/bin/env python
import typer
import json
app = typer.Typer()


@app.command()
def sql(file_name: str):
    with open(file_name) as file:
        table_names = json.load(file)
    
    assert isinstance(table_names, list), "deve ser uma lista"


if __name__ == "__main__":
    app()
