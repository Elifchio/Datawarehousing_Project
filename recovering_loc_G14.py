from os import chdir, getcwd, replace
getcwd()
chdir('C:/Users/alix2/Desktop/dss PROJECT')

#delete old variable if any
#%reset -f 

#use csv library to add new locations
import csv

#recover the location only after having cleaned the Crashes file
#use the Cleaned Crashes1 file for recovering locations
file_path='C:/Users/alix2/Desktop/dss PROJECT/Crashes1.csv'

#recovering location
from geopy.geocoders import Nominatim  

#use it for avoid having a crash in the request for finding locations
import time 

temp_file = 'temp.csv'

with open(file_path, 'r', encoding='utf-8', newline='') as file, \
    open(temp_file, mode='w', newline='', encoding='utf-8') as file_loc:
        csv_dict = csv.DictReader(file, delimiter=',')
        fieldnames = csv_dict.fieldnames
        csv_writer = csv.DictWriter(file_loc, fieldnames=csv_dict.fieldnames)
        csv_writer.writeheader()
        
        #initialize geolocator
        geolocator = Nominatim(user_agent="conda_spyder", timeout=10)  
    
        for rec in csv_dict:
            # If the location of a crash is unknown, and we have street info, try to geocode
            if rec.get('LOCATION') == 'UNKNOWN' and rec.get('STREET_NO') != '-1' and rec.get('STREET_NAME') != 'UNKNOWN':
                loc_string = f"{rec['STREET_NO']} {rec['STREET_NAME']}, CA, Chicago, USA"
                location = geolocator.geocode(loc_string)
                time.sleep(1) #to avoid crashing the process even it will take more time
                
                if location:  
                    rec['LATITUDE'] = str(location.latitude)
                    rec['LONGITUDE'] = str(location.longitude)
                    rec['LOCATION'] = f"POINT ({location.latitude} {location.longitude})"
    
            csv_writer.writerow(rec) #write the updated dictionary with the recovered locations into the temp file
replace(temp_file , file_path) #add recovered location to the existing file
print(f"File '{file_path}' with filled location correctly saved")

