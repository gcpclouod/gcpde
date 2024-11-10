[
  { "name": "id", "type": "INTEGER" },
  { "name": "name", "type": "STRING" },
  { "name": "age", "type": "INTEGER" },
  { "name": "joining_date", "type": "DATE" },
  { "name": "joining_time", "type": "TIME" },
  { "name": "salary", "type": "FLOAT" },
  { "name": "is_active", "type": "BOOLEAN" }
]


bq mk --table [PROJECT_ID]:[DATASET].[TABLE_NAME] schema.json

bq mk --table woven-name-434311-i8:cnn_project.emp_table schema.json