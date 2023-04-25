src_file = olddb.sql

convert:
	python3 migration_script/postgresql2mysql.py $(src_file)