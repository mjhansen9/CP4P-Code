import pandas
import datetime
from dateutil import parser
from datetime import datetime, date, timedelta


def format_date(date_str, placeholder="3000-01-01"):
    try:
        # Automatically parse the date using dateutil's parser

        if pandas.isna(date_str):
            return placeholder  # Return None if the date is missing

        date_str = date_str.strip()
        parsed_date = parser.parse(date_str)
        return parsed_date.strftime('%Y-%m-%d')
    except Exception as e:
        # Handle exceptions if the date format is unrecognized
        print(f"Error parsing date: {date_str}, Error: {str(e)}")
        return

def float_x(funding_lvl):
    try:
        return float(funding_lvl)
    except:
        return None

    
    
    
#now continue same process with final sheet, using the previous sheet as a base.

FY_2025_july_dec_old_format = pandas.read_excel("C:\\Users\\HansenMade\\Downloads\\000-Staff Listing New 06.05.24(10)_TEST.xlsx",sheet_name = 'FY25 July-Dec')
FY_2025_july_dec_old_format.replace('#','', inplace=True,regex = True)
FY_2025_july_dec_old_format.rename(columns={'Staff Name':'Staff_Name','Caseload Budget Y/N':'Case_Load'},inplace=True)
FY_2025_july_dec_old_format[['Provider','Site']] = FY_2025_july_dec_old_format['Provider'].str.split('-',expand = True)
FY_2025_july_dec_old_format['Site'] = FY_2025_july_dec_old_format['Site'].fillna('N/A')
#above splits the provider column (formatted as 'ORGANIZATION_1 - EGP' for example) into 2 columns for provider and site
#then fills with 'n/a' as none doesnt = none
FY_2025_july_dec_old_format = FY_2025_july_dec_old_format.map(lambda x: x.strip() if isinstance(x,str) else x)
#cleans the string columns of whitespaces
FY_2025_july_dec_old_format = FY_2025_july_dec_old_format[FY_2025_july_dec_old_format['Purchase No.'].notna() & (FY_2025_july_dec_old_format['Purchase No.'] != '')]  
#removes instances where funder is blank


FY25_July_Dec_new_format = FY_2025_july_dec_old_format[['Provider','Site','Staff_Name','Title','Case_Load']].copy()
FY25_July_Dec_new_format.drop_duplicates(subset = ['Provider','Site','Staff_Name','Title'],inplace = True,ignore_index = True)

purchase_no = FY_2025_july_dec_old_format['Purchase No.'].unique()
FY25_July_Dec_new_format.insert(len(FY25_July_Dec_new_format.columns),'FROM_DATE',None)
FY25_July_Dec_new_format.insert(len(FY25_July_Dec_new_format.columns),'TO_DATE',None)
#gets a unique list of all possible funding sources
for i in purchase_no:
    FY25_July_Dec_new_format.insert(len(FY25_July_Dec_new_format.columns),i,None)
    
    
staff_allocations_copy = FY25_July_Dec_new_format.copy()


column_list = FY_2025_july_dec_old_format.columns
date_col_list=column_list[8:20]

FY25_July_Dec_new_format = pandas.read_excel("C:\\Users\\HansenMade\\Python Files\\python scripts\\TEST_3.xlsx")

FY_2025_july_dec_old_format = FY_2025_july_dec_old_format[FY_2025_july_dec_old_format['Purchase No.'].notna() & (FY_2025_july_dec_old_format['Purchase No.'] != '')]  


#below adds any new funding source to existing df
purchase_no = FY_2025_july_dec_old_format['Purchase No.'].unique()
for i in purchase_no:
    if i not in FY25_July_Dec_new_format:
        FY25_July_Dec_new_format.insert(len(FY25_July_Dec_new_format.columns),i,None)
        
FY25_July_Dec_new_format[purchase_no] = FY25_July_Dec_new_format[purchase_no].fillna(0)



for date_column in range(len(date_col_list)):
    #print(date_col_list[date_column])
    #loops remaining date columns, updating the dataframe with data as it changes
    new_df = FY_2025_july_dec_old_format[['Purchase No.','Provider','Site','Staff_Name','Case_Load','Title',date_col_list[date_column]]].copy()
    #grabs data from the date column currently being worked with in loop
    new_df[date_col_list[date_column]] = new_df[date_col_list[date_column]].apply(float_x)
    new_staff_allocations = staff_allocations_copy.copy()
    for i in range(len(new_df)):
        #loc the new_staff_allocations df to where it matches the new dataframe on provider, site, Staff_Name and staff role
        new_staff_allocations[new_df.iloc[i]['Purchase No.']][(new_staff_allocations['Provider'] == new_df.iloc[i]['Provider']) & (new_staff_allocations['Site'] == new_df.iloc[i]['Site']) & (new_staff_allocations['Title'] == new_df.iloc[i]['Title']) & (new_staff_allocations['Staff_Name'] == new_df.iloc[i]['Staff_Name'])] = new_df.iloc[i][date_col_list[date_column]]

    
    if date_column == 0:
        #the first date column does not have a prior dare column
        first_from_date_month = date_col_list[date_column].month 
        first_from_date_year = date_col_list[date_column].year 
        new_staff_allocations['FROM_DATE'] = str(first_from_date_year) + '-' + str(first_from_date_month) + '-01'
        new_staff_allocations['FROM_DATE'] = new_staff_allocations['FROM_DATE'].apply(format_date)
        #as the first date column doesn't have a previous column to base it's from date on, creates one. assumes the first date column is on a 15th
        new_staff_allocations['TO_DATE'] = '3000-01-01'
    else:
        new_staff_allocations['FROM_DATE'] = str(date_col_list[date_column-1]+timedelta(days = 1))
        #sets from date of new data to the begining of the period, which is the previous column's date + 1 day
        new_staff_allocations['FROM_DATE'] = new_staff_allocations['FROM_DATE'].apply(format_date)
        new_staff_allocations['TO_DATE'] = '3000-01-01'
        
    
    new_staff_allocations[purchase_no] = new_staff_allocations[purchase_no].fillna(0)
    #fills empty cpots with 0s
    #compare against existing apply from and to date logic FY25_July_Dec_new_format 

    existing_staff_allocations = FY25_July_Dec_new_format[FY25_July_Dec_new_format['TO_DATE'] == '3000-01-01'].copy()
    #grabs all the current records (aka those with to dates of 3000) to compare against newer data
    if date_column == 0:
        previous_day = date(date_col_list[date_column].year,date_col_list[date_column].month,1)-timedelta(days = 1)
        existing_staff_allocations['TO_DATE'] = str(previous_day.year) + '-' + str(previous_day.month) + '-' + str(previous_day.day)
    else:
        existing_staff_allocations['TO_DATE'] = str(date_col_list[date_column-1])
    #sets to date of current records to that of the end of the previous perio
    existing_staff_allocations['TO_DATE'] = existing_staff_allocations['TO_DATE'].apply(format_date)
    updated_staff_allocations = pandas.concat([new_staff_allocations,existing_staff_allocations], ignore_index = True)
    columns_wo_from_to_date = list(updated_staff_allocations.columns)
    columns_wo_from_to_date.remove('TO_DATE')
    columns_wo_from_to_date.remove('FROM_DATE')
    updated_staff_allocations.drop_duplicates(subset=columns_wo_from_to_date, keep=False, inplace = True, ignore_index = True)
    #combines the newer data and the current data into one dataframe, with the newer data on top, then drops the duplicates from the combined dataframe based on all columns but from and to date.
    #this results in a df containing only data that has been altered between the current records and new records. the current records remaining in this df will be out of date records
    updated_staff_allocations = pandas.concat([updated_staff_allocations,FY25_July_Dec_new_format], ignore_index = True)
    FY25_July_Dec_new_format = updated_staff_allocations.drop_duplicates(subset=['FROM_DATE','Provider','Site','Title','Staff_Name'], keep='first', ignore_index = True)
    #combines the df containing only updated and out of date records with the entirety of the existing df. then deletes duplicates based on from_date and all other fields used to uniquely id a record. 
    #this adds the new records and the out of date records and removes the previously current, now out of date records
    
    
    
FY25_July_Dec_new_format.to_excel('UPDATED_TEST.xlsx',index = False)