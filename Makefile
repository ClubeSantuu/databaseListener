src_file = old_db.sql
final_file = result.sql

convert:
	python3 migration_script/postgresql2mysql.py $(src_file) $(final_file)