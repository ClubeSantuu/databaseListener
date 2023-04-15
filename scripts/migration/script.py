import json

fd = open('old_db_structure.json', 'r')
STRUCTURE = json.load(fd)
fd.close()

def get_string_between(text, val_start, val_end):
    try:
        return text.split(val_start)[1].split(val_end)[0]
    except:
        return None

def replace_string_between(text, val_start, val_end, final):
    start = text.split(val_start)[0]
    end = text.split(val_end)[1]
    return f"{start}{val_start}{final}{val_end}{end}"

def get_inserts_and_values(sql):

    result = []
    strs = sql.split("LOCK TABLES `")[1:]

    for text in strs:
        localized = text.split("UNLOCK TABLES;")[0]
        clean = f"LOCK TABLES `{localized}UNLOCK TABLES;"
        
        result.append(
            (
                clean,
                get_string_between(clean,"` VALUES ","\n")
            )
        )
    
    return result

def convert_values_in_insert(insert_sql, final_values):
    return replace_string_between(insert_sql, "` VALUES ","\n", final_values)

def get_field_by_table_name(table_name):
    try:
        table_structure = STRUCTURE[table_name]
    except KeyError:
        return None

    return f"({','.join(table_structure)})"

def replace_name_by_position(fields_to_remove):
    for table_name, table_values in fields_to_remove.items():
        for i, field in enumerate(table_values):
            table_values[i] = tuafuncao(field)
    return fields_to_remove

def get_field_position_to_remove(table_name):
    result = replace_name_by_position(
        {
            "voucher_voucher_shop_voucher": ["id"]
        }
    )
    return result[table_name]

def get_field_position_by_table_name(table_name: str, field: str):
    try:
        table_structure: list[str] = STRUCTURE[table_name]
        return table_structure.index(field) + 1
    except KeyError:
        return None
    except ValueError:
        return None

def take_away_field(values = "(null,null),(null,null)", position = []):
    values = values[0:-1] # tirando ;
    values = values + ",("
    values = values.replace("),(", ",---end---divider(")
    values = values.split("divider")
    values.pop() # pop porque o ultimo é vazio
    # termina com ',---end---' e começa com '('

    result_values = []
    
    for value in values:
        value = value[1:] # 1: para tirar o (
        actual_type = None
        clean_value = []

        field_position = 0

        there_is_next = True

        while there_is_next:
            field_position += 1
            if value[0]=="'":
                value = value[1:]
                actual_type = "string"
                separator = "',"
                complete_with = "'"
            else:
                actual_type = "number"
                separator = ","
                complete_with = ""

            clean, value =  value.split(separator, 1) # põe o valor em clean e o resto fica em field

            if position is None or not field_position in position:
                clean_value.append(f"{complete_with}{clean}{complete_with}")     
   
            there_is_next = value != "---end---"
            
        clean_value = f"({','.join(clean_value)})"
        result_values.append(clean_value)

    result_values = ",".join(result_values) + ";"
    
    return result_values

def get_table_name_from_insert(insert):
    return get_string_between(insert, "LOCK TABLES `", "`")

def convert_sql(sql):
    inserts_and_values = get_inserts_and_values(sql)
    final_inserts = []
    for insert, values in inserts_and_values:
        if values is None:
            continue
        table_name = get_table_name_from_insert(insert)
        if table_name is None:
            continue
        positions_to_remove = get_field_position_to_remove(table_name)
        values = take_away_field(values, positions_to_remove)
        result = convert_values_in_insert(insert, values)
        field_sequence = get_field_by_table_name(table_name)
        if field_sequence is None:
            continue
        field_sequence = take_away_field(field_sequence + ";", positions_to_remove)
        result = replace_string_between(result, "` ", "VALUES (", field_sequence + " ")
        final_inserts.append(result)
    return "\n\n".join(final_inserts)

fd = open('apidb.sql', 'r')
sql = fd.read()
fd.close()

converted = convert_sql(sql)
# print(converted)
breakpoint()
