# DBX_GENERATOR
Databricks generator


First using you must install these libraries
- pip install openpyxl
- pip install python-dotenv


Second change the BASE_PATH from the .env file
 - .env

Third from the context_parameter sheet change the context parameters values from the dbx_scehma.xlsx
 - Don't edit the red highligted column

Fourth from the schema sheet in dbx_scehma.xlsx
 - input your ddl (fields in the column B) and data type in column C 
 - You may add comments if needed (in the designated comment column).


Fifth always include the these fields at the bottom
- dbx_process_dttm
- file_name
- file_id



! Please note that you have to change the source partition list or target partition list from the JSON config file