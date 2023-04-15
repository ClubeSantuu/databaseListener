import argparse, json


parser = argparse.ArgumentParser(prog="PROG")

parser.add_argument("--pg", required=True)
parser.add_argument("--msql", required=True)

args = parser.parse_args()


def filter(file) -> dict:
    table = {}
    get = False
    name = ""
    for line in file.readlines():
        line:str = line.strip()

        if get:
            field = line.split(" ")[0].replace("`","")
            if field not in ['PRIMARY', 'UNIQUE', 'KEY', 'CONSTRAINT', 'CONSTRAINT']:
                table[name].append(field)

        if line.upper().startswith("CREATE TABLE"):
            get = True
            name = line.split(" ")[2].replace("`","").split(".")[-1]

            table[name] = []

        if get and line.startswith(")") and line.endswith(";"):
            get = False
            del table[name][-1]
            # res.append(table)

    return table


def save(file_name: str):
    with open(file_name) as file:
        obj = filter(file)

    with open(f"{file_name}.json", "w") as file:
        json.dump(obj, file, indent=4)


save(args.pg)
save(args.msql)
