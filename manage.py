#!/usr/bin/env python
import typer
import asyncio
import enum
import itertools as it

from listen.utils import read_tables
from listen.listen import main

app = typer.Typer()


DROP_TEMPLATE = """
    DROP TRIGGER IF EXISTS {trigger_name}
    ON {table_name};

    DROP FUNCTION IF EXISTS {function_name};
"""
CREATE_TEMPLATE = """
    CREATE OR REPLACE FUNCTION {function_name}()
        RETURNS trigger 
        language plpgsql
        AS
    $$
    BEGIN
    PERFORM pg_notify('{event_name}', row_to_json(NEW)::text);
    RETURN new;
    END;
    $$;

    CREATE OR REPLACE TRIGGER {trigger_name}
    AFTER {operation}
    ON {table_name}
    FOR EACH ROW
    EXECUTE PROCEDURE {function_name}();
"""

OPERATIONS = ["insert", "delete", "update"]

def get_context_data(args) -> dict:
    table_name, operation = args
    return {
        "trigger_name": f"trigger_{table_name}_{operation}",
        "operation": operation.upper(),
        "table_name": f"public.{table_name}",
        "function_name": f"public.function_{table_name}_{operation}",
        "event_name": f"event_{table_name}_{operation}",
    }

def render(ctx, template: str):
    txt = template.format(**ctx)

    return f"\n".join(map(str.strip, txt.split("\n")))


class Type(str, enum.Enum):
    drop = "DROP"
    create = "CREATE"


@app.command()
def sql(file_name: str, type: Type):
    template = ""
    if type == Type.drop:
        template = DROP_TEMPLATE
    elif type == Type.create:
        template = CREATE_TEMPLATE

    tables = read_tables(file_name)

    iterator = ((name, op) for name in tables for op in OPERATIONS)
    ctx = map(get_context_data, iterator)
    sql = "\n".join(map(lambda c: render(c, template), ctx))
    
    with open(f"{file_name}.{type}.sql".lower(), "w") as file:
        file.write(sql)
        print("salvo em:", file.name)

@app.command()
def listen(file_name: str):
    tables = list(read_tables(file_name))
    print(tables)
    asyncio.run(main(tables, OPERATIONS))


if __name__ == "__main__":
    app()
