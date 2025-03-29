#Creating table
from os import chdir, getcwd
getcwd()
chdir('C:/Users/alix2/Desktop/dss PROJECT')

#delete old variable if any
#%reset -f 

path_to_use='C:/Users/alix2/Desktop/dss PROJECT'
#put the data preparation output files
crf='Crashes1.csv'
vef='Vehicles1.csv'
ppf='People1.csv'

import csv #useful library 

'''
Before creating all the tables, we choose to merge the 3 files, connecting them
using the same crash in which they are involved, even it would takes to having a 
duplicated rd_no
Please include .csv, when calling a table name, in every function of this file
'''
rd= 'RD_NO'
merged_name ='Merged_Tables.csv' #when creating a name always insert .csv at the end to avoid .txt format


with open(path_to_use+'/'+crf, "r") as f_crashes, \
     open(path_to_use+'/'+vef, "r") as f_vehicles, \
     open(path_to_use+'/'+ppf, "r") as f_people:
    
    #in order to use the list of dictionaries with the closed files, we have to
    #use list comprehension (or also copying them is ok) 
    #now we have every file stored in a list of dictionaries        
    crashes = list(csv.DictReader(f_crashes))
    vehicles = list(csv.DictReader(f_vehicles))
    people = list(csv.DictReader(f_people))

'''
We start merging from people, so for one person we will have its infos, then also
the ones regarding the crash is was involved in, and also if any, the info regarding 
the vehicle (if it was an occupant for example of a motor vehicle), as there are some Nan values
'''

cr_key={} #initialize a dictionary for containing rd_no and their related record
for cr in crashes:
    cr_key[cr[rd]]= cr #map every crash using its rd_no identifier
    #no need a duplicate check, as rd_no is a crash' s identifier

pers_crash=people.copy()
for pers in pers_crash:
    pp_cr= {} #initilize a dictionary for merging person and crash' records
    if pers[rd] in cr_key.keys(): #a check to see if a person is connected to one rd
        pp_cr=cr_key[pers[rd]]
    pers.update(pp_cr)
#♠now a record of people is also containing the related crashes' infos

ve_key={} 
for ve in vehicles:
    
    vehicle_id = ve['VEHICLE_ID']
    rd_no = ve[rd]
    
    if vehicle_id != '-1': #valid vehicle_id, default case
        ve_key[(rd_no, vehicle_id)] = ve    
    else: continue #we are not dealing with a motor vehicle, so the dimensions will be useless        

merged= pers_crash.copy() #from now on we will have all the informations in this unique table
for pers in merged:
    
    rd_no = pers[rd]
    vehicle_id = pers['VEHICLE_ID']

    pp_cr= {} #initilize a dictionary for merging person and crash' records
    if vehicle_id == '-1': #it means we have a person who isn't related to a motor vehicle,
            ve_miss={} #put an missing dictionary and fill it with -1 or UNKNOWN
                       #because also the other columns values will be nan
                       #as the person is not related to some vehicle
            for col, val in vehicles[0].items():
                pers[col] = '-1' if val.isdigit() else 'UNKNOWN'                  
        
    else:
        conn_pp = (rd_no, vehicle_id)        
        if conn_pp in ve_key.keys(): #a check to see if a person is connected to one rd
            pp_cr=ve_key[conn_pp]
        else: #if no correspondence of vehicle_id
            for col, val in vehicles[0].items():
                pers[col] = '-1' if val.isdigit() else 'UNKNOWN'
        pers.update(pp_cr)

#an additional check again to see if there are still missing informations
for rec in merged:
    for key, value in rec.items():
        if value == '' or value is None: 
            rec[key] = '-1' if key.isdigit() else 'UNKNOWN' #mark them with usual unknown or -1
    
#now the list named people should contains for each entry, infos about, the person involved, the
#crash and the related vehicle, so we can create a new merged file
with open(merged_name , "w", newline="") as f_output:
    fieldnames = list(merged[0].keys())  #saving the dictionary keys (column names)

    writer = csv.DictWriter(f_output, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(merged)

print(f"{merged_name} correctly saved in this folder {path_to_use}")
        



'''
Here we created functions we will use: for creating primary key for all the dimension tables that
we will have. For the table that has RD_NO as key, we will create a different primary_key
based on the different combination of all the columns ' distinct values.
Then for the vehicle and people' tables  we will create also a surrogate key, because
the same person or vehicle can be involved in more than one crash.
Then we will obtain all the record using the merged table we created, so also a function
for creating all the table of our star schema
'''
   
#to turn the dictionary 's values into tuples if needed
#put cols only if some columns are needed
def from_dic_to_tp(dic_rec, cols=None):
    if cols is None: #case without any restriction on the columns
        rec_tp = tuple(dic_rec[k] for k in sorted(dic_rec))
    else:
        rec_tp = tuple(dic_rec[k] for k in sorted(cols))
    #use sorted in order to have a non abritary variables' values combination
    return rec_tp   


#this funtion store all' tables different combination of attributes
#cols: insert here the columns we want to have in a certain table
def list_of_combination(path, file_name, cols):
    #open the file from which we will take the desired columns
    import csv 
    with open(f"{path}/{file_name}", mode= 'r', encoding= 'utf-8', newline='') as file:
            dict_f= csv.DictReader(file, delimiter=',')
            list_records=set() #here we will store all the record having only the desired columns
            for rec in dict_f: #iterate over the records
                new_rec= {}  #initialize a record with only the columns needed
                
                for k,v in rec.items():
                    if k in cols: #take from the original file only the column needed in the table
                        new_rec[k]=v #record only with specified columns
                rec_tp= from_dic_to_tp(new_rec) #create a tuple so it can be inserted in a set
                list_records.add(rec_tp) #use set structure to avoid duplicated combinations
            list_no_dup=list(list_records) 
    return list_no_dup  


#create a dictionary that contains a incremental key for all the different tables'  combinations
def mapping_records(list_of_rec):
    map_dic={} #initialize empty dictionary
    i=1 #initialize a counter
    
    #now start scanning all the record
    for rec in list_of_rec:
        map_dic[rec]= i #creating a key to refer to each combination
        i +=1
    return map_dic #now for each combination we have an identifier

#this function create surrogate keys, using tuples
#insert columns ' names that together are primary key
def mapping_tuples(path, file_name, col1, col2): 
    import csv 
    with open(f"{path}/{file_name}", mode= 'r', encoding= 'utf-8', newline='') as file:
            dict_f= csv.DictReader(file, delimiter=',')
            map_dic= {}
            i= 1 #initialize a counter
            for rec in dict_f:
                k_tp= (rec.get(col1), rec.get(col2)) 
                #we create tuples because the key is composed by 2 attributes
                map_dic[k_tp]= i 
                i+=1
            #no need duplicated check as we know we are dealing with pk
            return map_dic

#this functions add all the creted dimensions table primary key to  the merged dictionary,
#it would be useful when dividing in all the datawarehouse' tables
def add_primary_keys(merged_dict, pk_map, pk_col_name, cols_for_pk):
    for rec in merged_dict:
        #save the records' combination of attribute
        pk_key = from_dic_to_tp(rec, cols= cols_for_pk) #take only the desired columns
        
        rec[pk_col_name] = pk_map.get(pk_key) #add the pk columns for each record, using the dictionary
    
    return merged_dict




#this function divides the merged records into all the tables that we created 
#cols parameter must include also the primary key
def creating_table(file_dic, new_t_name, path_to_save, cols):
    with open(f"{path_to_save}/{new_t_name}", mode= 'w', encoding= 'utf-8', newline='') as new_table:
            import csv
            dict_newt=csv.DictWriter(new_table, fieldnames=cols) 
            dict_newt.writeheader() #initialize our new table in a list of dictionary
            unique_records = set()  #initialize a set for avoiding any duplicate
                    
            for rec in file_dic:
                rec_tp = from_dic_to_tp(rec, cols)  # convert the record to a tuple, to be put in a set
                        
                if rec_tp not in unique_records:  #duplicate check
                    unique_records.add(rec_tp)  # add the tuple to the set 
                    new_rec = {k: rec[k] for k in cols}  #use only the specified columns
                    dict_newt.writerow(new_rec)    
    print(f'{new_t_name} correctly created and saved in {path_to_save}')

  
'''
Create primary key manually for all the dimension tables and add them to the merged dictionary (name: 'merged')
Then create the table by selecting only its columns
'''          

#Date Primary key
d_col=['CRASH_DATE', 'CRASH_HOUR', 'CRASH_DAY_OF_WEEK','CRASH_MONTH', 'DATE_POLICE_NOTIFIED']
d_list= list_of_combination(path_to_use, merged_name, d_col)
date_pk= mapping_records(d_list)
merged= add_primary_keys(merged, date_pk, 'DATE_PK', d_col)

#Date Table
d_col_key=['DATE_PK']+d_col
creating_table(merged, 'Date_table.csv', path_to_use, d_col_key)

#Geography Primary key
g_col= ['LOCATION', 'LATITUDE', 'LONGITUDE', 'STREET_NAME', \
        'ALIGNMENT', 'STREET_DIRECTION', 'STREET_NO', 'BEAT_OF_OCCURRENCE']
g_list= list_of_combination(path_to_use, merged_name, g_col)
geo_pk= mapping_records(g_list)
merged= add_primary_keys(merged, geo_pk, 'GEOGRAPHY_PK', g_col)

#Geography Table
g_col_key=['GEOGRAPHY_PK']+g_col
creating_table(merged, 'Geography_table.csv', path_to_use, g_col_key)


#Crash Primary key
c_col= ['POSTED_SPEED_LIMIT', 'TRAFFIC_CONTROL_DEVICE', 'DEVICE_CONDITION', 'TRAFFICWAY_TYPE', \
        'REPORT_TYPE', 'CRASH_TYPE', 'FIRST_CRASH_TYPE', 'ROAD_DEFECT', \
        'ROADWAY_SURFACE_COND', 'MOST_SEVERE_INJURY']
    
c_list= list_of_combination(path_to_use, merged_name, c_col)
crash_pk= mapping_records(c_list)
merged= add_primary_keys(merged, crash_pk, 'CRASH_PK', c_col)

#Crash Table
c_col_key=['CRASH_PK']+c_col
creating_table(merged, 'Crash_table.csv', path_to_use, c_col_key)

        
#Cause Primary key
ca_col=['PRIM_CONTRIBUTORY_CAUSE', 'SEC_CONTRIBUTORY_CAUSE', \
        'DRIVER_ACTION', 'DRIVER_VISION', 'PHYSICAL_CONDITION']
ca_list= list_of_combination(path_to_use, merged_name, ca_col)
cause_pk= mapping_records(ca_list)
merged= add_primary_keys(merged, cause_pk, 'CAUSE_PK', ca_col)

#Cause Table
ca_col_key=['CAUSE_PK']+ca_col
creating_table(merged, 'Cause_table.csv', path_to_use, ca_col_key)


#Weather Primary key
w_col= ['WEATHER_CONDITION', 'LIGHTING_CONDITION']
w_list= list_of_combination(path_to_use, merged_name, w_col)
wheather_pk= mapping_records(w_list)
merged= add_primary_keys(merged, wheather_pk, 'WEATHER_PK', w_col)

#Weather Table
w_col_key=['WEATHER_PK']+w_col
creating_table(merged, 'Weather_table.csv', path_to_use, w_col_key)


#Person Primary key
c1, c2= 'PERSON_ID', rd
person_pk= mapping_tuples(path_to_use, merged_name, c1, c2)
merged= add_primary_keys(merged, person_pk, 'PERSON_PK', [c1,c2])


#Person Table
p_col_key =['PERSON_PK', 'PERSON_ID', 'PERSON_TYPE', 'SEX', 'AGE', \
            'SAFETY_EQUIPMENT', 'EJECTION', 'INJURY_CLASSIFICATION', \
            'BAC_RESULT', 'AIRBAG_DEPLOYED', 'CITY', 'STATE']
creating_table(merged, 'Person_table.csv', path_to_use, p_col_key)


#Vehicle Primary keys
#here we need to deals with the case of not given vehicle_id
v_id, cr_id= 'VEHICLE_ID', 'CRASH_UNIT_ID' 
vehicle_pk= {} #initialize a dictionary for the pk
i= 1 #initialize a counter
for rec in merged:
    if rec.get(v_id) == '-1': continue 
    else:
        k_tp= (rec.get(rd), rec.get(v_id)) #key: vehicle_id and rd_no
        vehicle_pk[k_tp]= i 
        i+= 1
        #no need duplicated check as we know we are dealing with pk

#now we have to insert primary key based on the primary key
for rec in merged:
    #check if vehicle_id is nan or not
    if rec.get(v_id) == "-1": 
        k_tp = (rec.get(rd), rec.get(cr_id))
    else:  
        k_tp = (rec.get(rd), rec.get(v_id))
    
    #add pk values, based or on v_id or on 
    rec["VEHICLE_PK"] = vehicle_pk.get(k_tp)

#Vehicle table
v_col_key=['VEHICLE_PK', 'UNIT_TYPE',  'VEHICLE_ID', 'MAKE', 'MODEL', 'LIC_PLATE_STATE', 'MANEUVER', \
           'VEHICLE_YEAR', 'VEHICLE_TYPE', 'VEHICLE_DEFECT', 'VEHICLE_USE', \
           'TRAVEL_DIRECTION', 'FIRST_CONTACT_POINT', 'UNIT_NO']

#to avoid having nan values in pk we do this additional check and if necessary, if 
#one row will have a vehicle_pk as nan, we will delete thet row (as it won't be about any motor vehicle)
#so the vehicle dimension, will be an otpional one in this case
with open(path_to_use+'/'+'Vehicle_table.csv', mode='w', encoding='utf-8', newline='') as veh_t:
    #import csv #only if not imported before
        dict_ve=csv.DictWriter(veh_t, fieldnames=v_col_key) 
        dict_ve.writeheader() #initialize our new table in a list of dictionary
        unique_veh= set()
        
        #additional invalid key check, that was not necessary for the other tables
        for rec in merged:
            if rec.get('VEHICLE_PK') in [None,'', '-1']: 
                continue #skip the non vehicle record
                
            rec_tp = from_dic_to_tp(rec, cols=v_col_key)
            if rec_tp not in unique_veh:
                unique_veh.add(rec_tp) 
                new_rec = {k: rec[k] for k in v_col_key}  #use vehicle table columns
                dict_ve.writerow(new_rec)  
            
print(f'Vehicle_table.csv correctly created and saved in {path_to_use}')


#Fact Table: Damage to user
fact_col_meas= ['DAMAGE', 'NUM_UNITS', 'INJURIES_FATAL', 'INJURIES_INCAPACITATING', \
                'INJURIES_NON_INCAPACITATING' , 'INJURIES_REPORTED_NOT_EVIDENT', \
                'INJURIES_NO_INDICATION', 'INJURIES_TOTAL','OCCUPANT_CNT', 'DAMAGE_CATEGORY']
fact_col_key=sorted(['DATE_PK', 'GEOGRAPHY_PK', 'CRASH_PK', 'CAUSE_PK', \
                     'WEATHER_PK', 'PERSON_PK', 'VEHICLE_PK']) + fact_col_meas

    
#we need to modify the primary key columns' names and set them as foreign key
with open(path_to_use+'/'+ 'Damage_To_User_table.csv', mode='w', encoding='utf-8', newline='') as fact_f:
    fact_new_name= sorted(['DATE_FK', 'GEOGRAPHY_FK', 'CRASH_FK', 'CAUSE_FK', \
                         'WEATHER_FK', 'PERSON_FK', 'VEHICLE_FK']) + fact_col_meas
    fact_dic = csv.DictWriter(fact_f, fieldnames=fact_new_name)
    fact_dic.writeheader()
    
    #now copy the desired columns' values and modify the foreign key columns' name
    for record in merged:
        f_rec= {} 
        for col in record.keys():
            if col in fact_col_key and col[-2:]== 'PK': 
                #replace PK with FK
                fk_col = col.replace('PK', 'FK')
                
                #also here we put an additional check for avoiding nan vehicle_fk
                if fk_col== 'VEHICLE_FK' and record.get('VEHICLE_ID')=='-1':
                    f_rec['VEHICLE_FK']= -1
                else:
                    f_rec[fk_col] = record.get(col)
                    
            elif col in fact_col_key:
                f_rec[col] = record.get(col)
            
        fact_dic.writerow(f_rec)
        

print('Fact Table correctly saved in {}'.format(path_to_use))
    



