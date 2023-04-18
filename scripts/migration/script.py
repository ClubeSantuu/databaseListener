import json
import re

fd = open('db_data/olddb.sql', 'r')
SQL = fd.read()
fd.close()

fd = open('db_data/old_db_structure.json', 'r')
OLD_STRUCTURE = json.load(fd)
fd.close()

fd = open('db_data/new_db_proposal.json', 'r')
NEW_STRUCTURE = json.load(fd)
fd.close()

fd = open('db_data/table_name_translation.json', 'r')
TABLE_TRANSLATION = json.load(fd)
fd.close()

fd = open('db_data/field_name_translation.json', 'r')
FIELD_TRANSLATION = json.load(fd)
fd.close()

INSERT_START_STR = "-- Copiando dados para a tabela public."
INSERT_END_STR = "ENABLE KEYS */;"
VALUES_START_STR = " VALUES\n\t"
VALUES_END_STR = ";\n/*!"

def get_string_between(text, val_start, val_end):
    try:
        return text.split(val_start)[1].split(val_end)[0]
    except:
        return None

def exists_in_new_db(table_name):
    return table_name in NEW_STRUCTURE

def replace_string_between(text, val_start, val_end, final):
    try:
        start = text.split(val_start, 1)[0]
        end = text.split(val_end, 1)[1]
    except IndexError:
        return ""
    return f"{start}{val_start}{final}{val_end}{end}"

def get_table_name_from_insert(insert):
    return get_string_between(insert, INSERT_START_STR, ":")

def get_field_by_table_name(table_name):
    try:
        table_structure = OLD_STRUCTURE[table_name]
    except KeyError:
        return None

    return f"(`{'`, `'.join(table_structure)}`)"

def remove_repeated_replace_into(insert, table_name, field_sequence):

    string_to_remove = ";\nREPLACE INTO \"{}\" {} VALUES\n\t".format(
        table_name,
        field_sequence.replace("`","\"")
    )
    string_first_replace_into = "*/" + string_to_remove

    insert = insert.replace(string_first_replace_into, "[[[FIRST_REPLACE_INTO]]]")
    insert = insert.replace(string_to_remove, ",\n\t")
    insert = insert.replace("[[[FIRST_REPLACE_INTO]]]", string_first_replace_into)
    return insert

def remove_comments(sql):
    sql = re.sub(r'--.*\n', "", sql)
    sql = re.sub(r'/\*.*\*/;', "", sql)
    return sql

def get_inserts_and_values(sql):

    result = []
    strs = sql.split(INSERT_START_STR)[1:] # [0] > antes do primeiro insert

    for text in strs:
        localized = text.split(INSERT_END_STR)[0]
        table_name = localized.split(":")[0]
        field_sequence = get_field_by_table_name(table_name)

        if not exists_in_new_db(translate_table_name(table_name)):
            print(table_name + " não está presente no novo banco")
            continue

        clean = f"{INSERT_START_STR}{localized}{INSERT_END_STR}"
        clean = remove_repeated_replace_into(clean, table_name, field_sequence)

        result.append(
            (
                clean,
                get_string_between(clean,VALUES_START_STR,VALUES_END_STR)
            )
        )

    return result

def replace_values_in_insert(insert_sql, final_values):
    return replace_string_between(insert_sql, VALUES_START_STR,VALUES_END_STR, final_values)

def replace_name_by_position(fields_to_remove):
    for table_name, table_values in fields_to_remove.items():
        for i, field in enumerate(table_values):
            table_values[i] = get_field_position_by_table_name(table_name, field)
    return fields_to_remove

def get_field_position_to_remove(table_name):
    try:
        return REMOVED_FIELDS[table_name]
    except KeyError:
        return None

def translate_field_name(table_name, field_name):
    try:
        return FIELD_TRANSLATION[table_name][field_name]
    except:
        return field_name

def translate_table_name(table_name):
    try:
        return TABLE_TRANSLATION[table_name]
    except:
        return table_name

def get_field_position_by_table_name(table_name: str, field: str):
    try:
        table_structure: list[str] = OLD_STRUCTURE[table_name]
        return table_structure.index(field) + 1
    except KeyError:
        return None
    except ValueError:
        return None

def take_away_field_from_field_list(table_name, fields = "(`id`,`name`,`count`)", position = []):
    fields = fields.split("`, `")
    fields[0] = fields[0][2:]
    fields[len(fields)-1] = fields[len(fields)-1][0:-2]

    result = []

    for i, field in enumerate(fields):
        if position is None or not (i+1) in position:
            result.append(translate_field_name(table_name, field))

    result = f"(`{'`,`'.join(result)}`)"
    return result


def take_away_field(table_name, values = "(null, null),\n\t(null, null);", positions_to_remove = []):
    if values[-1:] == ";":
        values = values[0:-1] # tirando ;
    values = values + ",\n\t(" # para o último ser descartado [..., ?]
    values = values.replace("),\n\t(", ", ---end---divider(")
    values = values.split("divider")
    values.pop() # pop porque o ultimo é vazio
    # termina com ',---end---' e começa com '('

    result_values = []

    for value in values:
        value = value[1:] # 1: para tirar o (
        actual_type = None
        clean_values = []

        field_position = 0

        there_is_next = True

        while there_is_next:
            field_position += 1
            if field_position == 1:
                if value[0]=="'": # primeiro campo não tem espaço
                    value = value[1:]
                    actual_type = "string"
                    separator = "', "
                    complete_with = "'"
                else:
                    actual_type = "not a string"
                    separator = ", "
                    complete_with = ""
            else:
                if value[0]=="'":
                    value = value[1:]
                    actual_type = "string"
                    separator = "', "
                    complete_with = "'"
                else:
                    actual_type = "not a string"
                    separator = ", "
                    complete_with = ""

            clean, value =  value.split(separator, 1) # põe o valor em clean e o resto fica em field
            clean = clean.replace("--", "[[[[PROHIBITED_DIGIT_1]]]]").replace("---", "[[[[PROHIBITED_DIGIT_2]]]]") # lida com -- e ----

            if positions_to_remove is None or not field_position in positions_to_remove:
                if clean == "false":
                    clean = 0
                elif clean == "true":
                    clean = 1
                elif clean[-3:] == "+00":
                    clean = clean[0:-3]
                elif clean.count("-") == len(clean):
                    clean = "NULL"
                    complete_with = ""

                clean_values.append("{}{}{}".format(
                    complete_with,
                    str(clean).replace("'", "[[[[ASPAS]]]]"),
                    complete_with,
                ))


            there_is_next = value != "---end---"

        clean_values = f"({','.join(clean_values)})"
        result_values.append(clean_values)

    result_values = ",\n\t".join(result_values)

    return result_values

def convert_sql(sql):
    inserts_and_values = get_inserts_and_values(sql)
    final_inserts = []
    for insert, values in inserts_and_values:
        table_name = get_table_name_from_insert(insert)

        if values is None:
            continue
        if table_name is None:
            continue

        field_sequence = get_field_by_table_name(table_name)
        if field_sequence is None:
            continue

        positions_to_remove = get_field_position_to_remove(table_name)
        field_sequence = take_away_field_from_field_list(table_name, field_sequence, positions_to_remove)
        values = take_away_field(table_name, values, positions_to_remove)

        result = replace_values_in_insert(insert, values)

        result = replace_string_between(result, "INTO \"" + table_name + "\" ", "VALUES", field_sequence.replace("\"", "`") + " ")
        result = result.replace("INTO \"" + table_name + "\"", "INTO `" + translate_table_name(table_name) + "`")
        final_inserts.append(result)
    return "\n\n".join(final_inserts)

fd = open('db_data/removed_fields.json', 'r')
REMOVED_FIELDS = json.load(fd)
REMOVED_FIELDS = replace_name_by_position(REMOVED_FIELDS)
fd.close()

converted = convert_sql(SQL)
converted = remove_comments(converted).replace("[[[[ASPAS]]]]", '\\\'')
converted = converted.replace("[[[[PROHIBITED_DIGIT_1]]]]","--")
converted = converted.replace("[[[[PROHIBITED_DIGIT_2]]]]","---")
converted = f"""
SET FOREIGN_KEY_CHECKS=0;
SET time_zone = '+0:00';
/*set global max_allowed_packet=128*1024*1024;*/
ALTER DATABASE ecosystemdb CHARACTER SET utf8 COLLATE utf8_general_ci;
ALTER TABLE core_model CONVERT TO CHARACTER SET utf8 COLLATE utf8_general_ci;

{converted}
"""

with open("db_data/proposal.sql", "w") as file:
    file.write(converted)

# arrumar questão das aspas duplas
# field_list repetido