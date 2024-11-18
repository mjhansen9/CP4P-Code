#goal: create lat and long columns in df based on exosting address column 
from geopy.geocoders import Nominatim
import pandas
from datetime import datetime


def add_lat_long(file,address_column,add_city_state = False,after_date = False,date_column = None):
    """takes in an excel sheet and generates latitude, longitude, and community based off the specified address column. then re-exports to the original file name"""
    print(file)
    df = pandas.read_excel(file)
    if 'Latitude' not in df.columns:
        df['Latitude'] = None
    if 'Longitude' not in df.columns:
        df['Longitude'] = None
    if 'Community Area' not in df.columns:
        df['Community Area'] = None
    if after_date != False:
        converted_date_after_date = datetime.strptime(after_date,'%Y-%m-%d')
    geolocator = Nominatim(user_agent="measurements",timeout = 20)
    #from testing Nominatim seemed more accurate than photon and provides more utility (like being able to pull community)
    #downside is it is less tolerant of misspelled addresses
    null_list_address = df[address_column].isnull()
    null_list_lat = df['Latitude'].isnull()
    null_list_long = df['Longitude'].isnull()
    null_list_comm = df['Community Area'].isnull()
    for row in range(len(df)):
        #for row in range(20):
        if null_list_address[row] == False and (null_list_lat[row] or null_list_long[row] or null_list_comm[row]):
            if after_date == False or df[date_column][row] >= converted_date_after_date:
                #only performs the address search on rows where lat/long/community is blank (and thulocs needs to be generated) and the address is present
                cleaned_address, seperator, additional_information = df[address_column][row].partition('(')
                #above added as many of my addresses comtains addtional details in parentheses which would disrupt the geolocation
                if add_city_state:
                        cleaned_address += ' Chicago IL'
                #if the address contained only the street address (as my data did) you can set add_city_state to True to have it add Chicago IL as the city and state. 
                try:
                    location = geolocator.geocode(cleaned_address, addressdetails = True)
                    #print(location.address)
                    #print(location.latitude, location.longitude)
                    df.loc[row,'Latitude'] = location.latitude
                    df.loc[row,'Longitude'] = location.longitude
                    try:
                        #as quarter does not always exist in addr pulled, it is put in its own try except block so lat and long can still be generated
                        df.loc[row,'Community Area'] = location.raw['address']['quarter']
                    except:
                        #the field neighborhood also may or may not exist. quarter is prioritized over neighborhood if both exist as neighborhood seems to correlate more with sub-neighborhoods (ex tri-taylor) and in my
                        #own data I've seen out of date names for neighborhoods stored in the neighborhood field, with the more modern name in the quarter field (did you know south lawndale used to be named 'Bohemian California'???)
                        try:
                            df.loc[row,'Community Area'] = location.raw['address']['neighbourhood']
                        except:
                            continue
                except:
                    print("unable to find address for",df[address_column][row])
                    #if location is unable to be found it will throw an error and this block will execute
                    continue
    df.to_excel(file,index = False)
	

             
add_lat_long("C:\\Users\\HansenMade\\Python Files\\ORGANIZATION_1 Compiled Data\\cleaned\\ORGANIZATION_1_incident_by_incident.xlsx",'Address/Cross Streets',add_city_state = True,after_date = '2024-10-01' ,date_column = 'Date of Incident  ↑')
add_lat_long("C:\\Users\\HansenMade\\Python Files\\ORGANIZATION_2 compiled data\\Cleaned\\ORGANIZATION_2 Incidents.xlsx",'Address/Cross streets',add_city_state = True,after_date = '2024-10-01', date_column = 'Date of Activity')