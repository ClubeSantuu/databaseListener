#from unidecode import unidecode
import codecs

_from = to = "utf-8"

#file_names = ["task_task_log.sql", "insurance_proposal_coverage.sql"]
file_names = ["olddb.sql"]

for file_name in file_names:
    with open("db_data/" + file_name, 'r') as file:
        
        sql = file.read()
        sql = sql.encode(_from).decode(to)
        
        #saved = unidecode(sql, "preserve")
        with codecs.open("db_data/converted_" + file_name, 'w', "utf-8") as f:
            f.write(sql)