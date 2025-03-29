#if any old variable to delete:
#%reset -f 

#set working directory where there are stored all the tables
from os import chdir, getcwd
getcwd()
chdir('C:/Users/alix2/Desktop/dss PROJECT')
path_to_use= 'C:/Users/alix2/Desktop/dss PROJECT'

#use pyodbc library to connect to the server
import pyodbc

#we also use csv library to transform them into list of dictionaries before uploading them
import csv

#try to connect to server using the connection string with the sql server authentication
try:
    connection_string = (
        "DRIVER={ODBC Driver 17 for SQL Server};"
        "SERVER=tcp:lds.di.unipi.it;"
        "DATABASE=Group_ID_14_DB;"
        "UID=Group_ID_14;"
        "PWD=XXXXXXXX;"
    )
    connection = pyodbc.connect(connection_string)
    print("Connection successful!")
    
#if any error during the connection see what they are
except pyodbc.Error as ex:
    sqlstate = ex.args[0]
    error_message = ex.args[1]
    print(f"SQLState: {sqlstate}")
    print(f"Error: {error_message}")

connection = pyodbc.connect(connection_string) 

#if connection worked, create a cursor for start querying the databases
cursor = connection.cursor()
    
#optional: enable fast execution mode
cursor.fast_executemany = True

#function to read CSVs into a dictionary
def read_csv_to_dict(path, file_name):
    with open(f"{path}/{file_name}", mode='r', encoding='utf-8') as file:
        reader = csv.DictReader(file)
        #copy all the variables' names in lower case, as they are in the ssms server
        reader.fieldnames = [col.lower() for col in reader.fieldnames]
        
        return [row for row in reader] #returning a list of dictionaries, for every csv file


# create dictionary for loading all the tables 
datatoload = {
    "Crash": read_csv_to_dict(path_to_use, "Crash_table.csv"),
    "Weather": read_csv_to_dict(path_to_use, "Weather_table.csv"),
    "Vehicle": read_csv_to_dict(path_to_use, "Vehicle_table.csv"),
    "Person": read_csv_to_dict(path_to_use, "Person_table.csv"),
    "Date": read_csv_to_dict(path_to_use, 'Date_table.csv'),
    "Cause" : read_csv_to_dict(path_to_use, 'Cause_table.csv'),
    "Geography" : read_csv_to_dict(path_to_use, 'Geography_table.csv'),
    "Damage_To_User" : read_csv_to_dict(path_to_use, 'fact1.csv')  
}

#as we enconteur some problem while uploading table we choose to upload every table
#split in different package of same size
data_size_to_load = 10000  

for table_name, rows in datatoload.items():
    if rows:  
        columns = ",".join(rows[0].keys())  #prepare the correct no of columns
        placeholders = ",".join("?" for _ in rows[0].keys())  # prepare the space for the values to insert using '?'
        sql = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"

        for i in range(0, len(rows), data_size_to_load): #use the size as the 'step' of for loop for dividing all the data
            batch = rows[i:i + data_size_to_load]  
            #prepare the data as tuples for inserting them
            cache = [tuple(row.values()) for row in batch]  
            try:
                cursor.executemany(sql, cache)  #uploading each package of datas
                print(f"Successfully uploaded package {i // data_size_to_load + 1} for '{table_name}'")
            except Exception as e:
                print(f"An error occurred while the uploading of package {i // data_size_to_load + 1} for '{table_name}': {e}")

 





#we also create empty table to be populated with SSIS
t_names= [ 'Weather', 'Vehicle', 'Person', 'Geography',
          'Date', 'Cause', 'Damage_To_User', 'Crash']

def create_empty_ssis_tables(connection, list_t_names):

    cursor = connection.cursor() #open a new cursor
    for t in list_t_names: #create every empty table 
       
        new_t_name = t + "_SSIS"
        query = f"SELECT TOP 0 * INTO [Group_ID_14].[{new_t_name}] FROM {t};"
        
        try:
            
            cursor.execute(query)
            print(f"Empty table '{new_t_name}' correctly created on  server")
        except Exception as e:
            print(f"An error occurs while creating '{new_t_name}': {e}")
    


create_empty_ssis_tables(connection, t_names)

#to save all our operation on sql server, commit the connection before closing it
connection.commit() 


#ad the very end of our operations, close the cursor and also the connection
cursor.close()
connection.close()
