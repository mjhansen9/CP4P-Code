#ORGANIZATION_1 Violent Incident Counter

#goal: take in compilation of BT's violent incidents (which are submitted by victim, not by incident, and return another compilation in which
#each row is a unique incident, with columns tracking the different victims. It retains all other information pertaining to the incident as well
#believe I can do this by modifying the combine_duplicate_row function I've already written, but having it it do a special action with the victim columns

#run combine_duplicate_row check based on date of incident, then do an l_d check on the address (bc I can see its common for the same address to be spelled slightly differently)
# or maybe just display the incidents and have the user donote them as the same incident or not? since the same addres can be written in ways that willl evade an l_d check (ie the numerical address v the intersection of streets)
#issue: if an incident is erronously matched, it wont check itself against other incidents: ie if 2 incidents occurred on Jan 4th with mult victims in each, it will only check the first in the list against the
# others, and the other incident wont match to itself.

#potential solution: have the check be run on the 1st row in a df every time, and once its matched, remove it and all of its matches from the list. thus the non dups will not be removed, this will take longer than 
#original function, but this list is not so long

#for victims, have it count the number of unique VI ids for an incident and that is the total victim count. then, have it run a for loop over the number of listed vitim ages and add col victim age 1, victim age 2, etc. just places
# the age for that victim in the col if already existing

from geopy.geocoders import Nominatim
import glob
import pandas
import os
import re
from rapidfuzz import fuzz
import sqlite3 
from sqlalchemy import create_engine, text
import warnings
from openpyxl import load_workbook
import xlrd
import json
from datetime import date
import datetime
warnings.simplefilter("ignore", category=FutureWarning)    
pandas.options.mode.chained_assignment = None  


def manual_data_review():
    """prompts user for manual review of data, re-calls itself if there is user error in repsonse"""
    response = input().lower()
    if response == 'a':
        return 'a'
    elif response == 'b':
        return 'b'
    elif response == 'error':
        return 'error'
    elif response == 'both':
        return 'both'
    else:
        #in case user entering a response that is not a valid input, recursively calls function and prompts user for correct input
        print('please re-enter response')
        return manual_data_review()


def manual_data_review_location():
    """prompts user for manual review of data, re-calls itself if there is user error in repsonse"""
    response = input().lower()
    if response == 'y':
        return 'Y'
    elif response == 'n':
        return 'N'
    else:
        #in case user entering a response that is not a valid input, recursively calls function and prompts user for correct input
        print('please re-enter response')
        return manual_data_review_location()

"""known quirks with below function: combine_duplicate_row:"""

#if a column has 'date' in the column name but the values within the column are not dates (or in a date format that is not either m/d/y or y/m/d h:m:s), all values in column will be erased during attempt
#to convert them to a general date format. current fix is just to manually chnage column's name or date format before running program

#if a text cell has double quotation marks within (" ") it breaks the SQL search. current fix is to cntrl+f remove all quotation marks from file before running program

def add_submission_date(current_date,dataframe):
    """adds a column to an excel file containing a 'submission date' based off the name of the folder it was in, if date cannot be
    automatically gathered from the filename, prompts user review"""
    if current_date.month:
        date = str(current_date.year) +str(current_date.month) + '10'
    if date!='':
        dataframe['Submission Date'] = date
        dataframe['Submission Date'] = pandas.to_datetime(dataframe['Submission Date'], format='%Y%m%d')
    return dataframe

def combine_duplicate_row(file, index_columns = [],select_latest=['Submission Date'],keep_all=[],keep_null=False,l_d_check = [],ignore=['file origin','sheetname','Organization','Form'],sort_by='Submission Date',sort_dir=False,combine_data=['file origin']):
    """searches through an excel file to find duplicates in specified col(s), combines the data of the duplicate rows. prompts user review in case of contradictory entries and allows them to either select the 
    correct date, mark it as an error or choose both. returns a new cleaned excel file with only unduplicated combined data and a new error file containing rows marked as erroroneous during user review"""
    
    #below creates a dataframe containing the data of specifies excel sheet, then 2 empty dfs that mirror the structure of the initial df
    excel_sheet = file
    for i in select_latest:
        if 'Old '+i not in excel_sheet.columns:
            excel_sheet['Old '+i] = ''
    if 'Transformations to Data' not in excel_sheet.columns:
        excel_sheet['Transformations to Data']=''
    if 'Total Victims' not in excel_sheet.columns:
        excel_sheet['Total Victims']=1
    elif 'Total Victims' in excel_sheet.columns:
        excel_sheet['Total Victims'].fillna(value = 1,inplace = True)
    if 'Age of Victim 1' not in excel_sheet.columns:
        excel_sheet.rename({'Age of victim':'Age of Victim 1'},inplace=True)
    
        
    
    final_df = excel_sheet.iloc[:0,:].copy()
    error_sheet = excel_sheet.iloc[:0,:].copy()
    
    #changes any dates to uniform format so as to ensure they can be checked correctly
    #for any index columns where the values are dates the format is changed to '%Y-%m-%d %H:%M:%S' as that is the required format for the later SQL search
    #2 different date formats are accounted for here: m/d/y and y/m/d h:m:s
    for column in excel_sheet.columns:
        if 'date' in column.lower():
            date_format1 = pandas.to_datetime(excel_sheet[column], errors='coerce', format='%m/%d/%Y')
            if column in index_columns:
                for i in range(len(date_format1)):
                    if type(date_format1[i]) != pandas._libs.tslibs.nattype.NaTType:
                        date_format1[i] = date_format1[i].strftime('%Y-%m-%d %H:%M:%S:%f')
                        #print(i)
            date_format2 = pandas.to_datetime(excel_sheet[column], errors='coerce', format='%Y-%m-%d %H:%M:%S')
            if column in index_columns:
                for i in range(len(date_format2)):
                    if type(date_format2[i]) != pandas._libs.tslibs.nattype.NaTType:
                        date_format2[i] = date_format2[i].strftime('%Y-%m-%d %H:%M:%S:%f')
            excel_sheet[column] = date_format1.fillna(date_format2)
            
    #sorts the data in specified manner (if possible)        
    if sort_by in excel_sheet.columns:
        excel_sheet.sort_values(by=sort_by,ascending=sort_dir, inplace=True)
        excel_sheet.reset_index(drop=True,inplace = True)
        SubDateInc = True
    else:
        SubDateInc = False
    if select_latest!=[] and SubDateInc == False:
        #if user has entered columns into select latest but the the sort_by col doesn't exist in the spreadsheet, points out issue to user and ends program
        print("unable to fufill select_latest condition as the table has no",sort_by,"column")
        return
        
    #creates a list to later loop over made of all column names except the index columns and the ignored columns
    column_names = list(excel_sheet.columns)
    for i in index_columns:
        if i in column_names:
            column_names.remove(i)
    for i in ignore:
        if i in column_names:
            column_names.remove(i)
    old_cols=[]
    for i in select_latest:
        if 'Old '+i in column_names:
            column_names.remove('Old '+i)
            old_cols.append('Old '+i)
    column_names.remove('Transformations to Data')
    column_names.remove('Total Victims')
    
    
    
    #below will return a series of T/F values denoting all duplicate values as True
    all_duplicate_check = excel_sheet.duplicated(keep=False,subset=index_columns)
    non_dups = []
    for i in range(len(all_duplicate_check)):
        if all_duplicate_check[i] == False:
            #all_duplicate_check[i] == False when it is an unduplicated value
            new_entry = excel_sheet.iloc[i]
            final_df.loc[len(final_df)] = new_entry
            #print(new_entry[index_columns])
            non_dups.append(i)
    #non_dups should now be a list containing the indexes of all unique (based on index cols) rows in the excel sheet
    #final_df should now contain all the rows from the original sheet that were unique (based on infex cols)
    
    excel_sheet.drop(non_dups,inplace=True)
    #excel_sheet.to_excel('TEST_DUP_'+file, index=False)
    #excel_sheet should only contain duplicate values at this point as the unique rows have been dropped
    excel_sheet.reset_index(drop=True,inplace = True)
    #reset the index as dropping rows does not reset the index of the remaining rows. 
    
    
    """
    first_duplicate_check = list(excel_sheet.duplicated(keep='first',subset=index_columns))
    #above will return a series of T/F values denoting duplicate values as True, exluding the first occurance of the duplicate value (which are False)
    #given that excel_sheet only contains duplicates, this list will help us run through all first instances of a duplicate in the given sheet
    """
    #conn = disk_engine.raw_connection()
    #cur = conn.cursor()
    #above commented out as I was having issues getting it to work, my goal was to remove the duplicates from the db after they've been 
    #worked on to hopefully speed up the sql query over the runtime. 
    #not sure how needed this optimization is or if it even would decrease runtime (might take more time deleting than would shave off searching)

    


    append_final_list = []
    append_error_list = []
    append_null_list=[]
    final_keep_list = []

    #old testing below, going to leave in in case future testing is needed
    #duplicated_entries = pandas.read_sql_query(f'SELECT * FROM excel_table WHERE "Event Date" LIKE "2022-08-05 00:00:00.000000" AND "Start Time" LIKE "16:00:00"',disk_engine)
    #print(duplicated_entries)
    
    accounted_for=[]
    
    for i in range(len(excel_sheet)):
        if i not in accounted_for:
            #this will loop over entire duplicate list, only performing action on the first instance of a particular duplicated row
            #the creation of the operator list will be used in making the SQL search. the operator list collects the operator needs to match the data types per index col
            operator_list = []
            for index in index_columns:
                operator = None
                if isinstance(excel_sheet[index][i], int) or (isinstance(excel_sheet[index][i], float) and excel_sheet[index].isnull().iloc[i]==False):
                    
                    operator = ' = '
                elif excel_sheet[index].isnull().iloc[i]:
                    operator = ' IS NULL '
                else:
                    operator = ' LIKE '
                operator_list.append(operator)
            
            print(i)
            #print(i) included to give user some visual feedback on that the program is running and how far along it is
            
            index_str = ''
            for j in range(len(index_columns)):
                #index_str should look like: "'Record: ID' LIKE 'aaa453gr' AND 'Age' LIKE 01/07/1990"...
                if operator_list[j] != ' IS NULL ':
                    index_str+= '"'+index_columns[j]+'"'+operator_list[j]+'"'+str(excel_sheet[index_columns[j]][i])+'"'
                else:
                    index_str+='"'+index_columns[j]+'"'+operator_list[j]
                if index_columns[j] != index_columns[-1]:
                    index_str+= ' AND '
            #print(index_str)
            
            #index_str is generated based on the data within the first instance of a duplicate anf the operator list created to match it
            #it is used to fill in the back half of a SQL query below
            error_sheet_copy = excel_sheet.iloc[i:].copy()
            disk_engine = create_engine('sqlite:///my_lite_store.db')
            error_sheet_copy.to_sql('excel_table', disk_engine, if_exists='replace',index=True)
            duplicated_entries = pandas.read_sql_query(f'SELECT * FROM excel_table WHERE {index_str}',disk_engine)
            #duplicated_entries is a dataframe 
            #print(duplicated_entries)
            if ' IS NULL ' in operator_list:
                for k in range(len(duplicated_entries)):
                        append_null_list.append(duplicated_entries['index'][k])
                continue
                #above makes it so that all rows with a null value in any index columns is removed from the duplicate matching process and added back to the final dataframe as is. this is to avoid
                #situations in which rows are matched erroneously since they both don't have values within the index columns
            for column in excel_sheet.columns:
                if column not in duplicated_entries.columns:
                    duplicated_entries[column]=''
            index_one = duplicated_entries['index'][0]
            if len(duplicated_entries)>1:
                total_vict=duplicated_entries['Total Victims'][0]
                Error = False
                #print(duplicated_entries.iloc[0])
                
                #below creation of a copy of duplicated_entries in case of error
                copy_df = duplicated_entries.copy()
                for j in range(1,len(duplicated_entries)):
                    index_two = duplicated_entries['index'][j]
                    try:
                        l_d1 = fuzz.ratio(duplicated_entries['Address/Cross Streets'][0].lower(), duplicated_entries['Address/Cross Streets'][j].lower()) 
                    except:
                        l_d1 = 100
                    try:
                        l_d2 = fuzz.ratio(duplicated_entries['Time of Incident'][0].lower(), duplicated_entries['Time of Incident'][j].lower()) 
                    except:
                        l_d2=100
                    if duplicated_entries['Age of alleged perpetrator'].isnull().iloc[0]==False and duplicated_entries['Age of alleged perpetrator'].isnull().iloc[j]==False and str(duplicated_entries['Age of alleged perpetrator'][j]).lower()!= str(duplicated_entries['Age of alleged perpetrator'][0]).lower():
                        check3 = False
                    else:
                        check3 = True
                    if duplicated_entries['Address/Cross Streets'][0]=='3000 W.Fulton':
                        print(l_d1)
                        print(l_d2)
                        print(check3)
                    if 70<=l_d1 and 95<=l_d2 and check3:
                        total_vict+=  duplicated_entries['Total Victims'][j]
                        accounted_for.append(index_two)
                    else:
                        """print(duplicated_entries['Date of Incident  ↑'][0])
                        print('Row 1')
                        print(duplicated_entries['Violent Incidents: Violent Incident #'][0])
                        print('street = ',duplicated_entries['Address/Cross Streets'][0])
                        print('time = ',duplicated_entries['Time of Incident'][0])
                        print('perp age = ',duplicated_entries['Age of alleged perpetrator'][0])
                        print('Row 2')
                        print(duplicated_entries['Violent Incidents: Violent Incident #'][j])
                        print('street = ',duplicated_entries['Address/Cross Streets'][j])
                        print('time = ',duplicated_entries['Time of Incident'][j])
                        print('perp age = ',duplicated_entries['Age of alleged perpetrator'][j])
                        print('Same Incident? Y/N')
                        local_check = manual_data_review_location()
                        if local_check=='Y':
                            total_vict+=duplicated_entries['Total Victims'][j]
                            accounted_for.append(index_two)
                        elif local_check=='N':"""
                        continue
                    
                    for column in column_names:
                        #print(column)
                        if column in l_d_check:
                                l_d = fuzz.ratio(str(duplicated_entries[column][j]).lower(), str(duplicated_entries[column][0]).lower()) 
                                #print(l_d)
                                if 85<=l_d:
                                    continue
                        #print(duplicated_entries.loc[0,column],duplicated_entries.loc[j,column])
                        dup_null = duplicated_entries[column].isnull()

                        #case 1 first entry is  null and second is not null: set entry 1 = entry 2
                        if column=='Age of Victim 1' and dup_null.iloc[j]==False:
                            if duplicated_entries['Age of Victim 1'].isnull().iloc[0]:
                                duplicated_entries.loc[0,'Age of Victim 1'] = duplicated_entries.loc[j ,'Age of Victim 1']
                            else:
                                k=2
                                found=False
                                while found==False:
                                    col_name = 'Age of Victim '+str(k)
                                    if col_name not in duplicated_entries.columns:
                                        duplicated_entries[col_name]=None
                                        duplicated_entries.loc[0,col_name] = duplicated_entries.loc[j ,'Age of Victim 1']
                                        if col_name not in excel_sheet.columns:
                                            excel_sheet[col_name]=None
                                        found=True
                                    elif duplicated_entries[col_name].isnull().iloc[0]:
                                        duplicated_entries.loc[0,col_name] = duplicated_entries.loc[j ,'Age of Victim 1']
                                        found=True
                                    else:
                                        k+=1
                            
                            
                        elif dup_null.iloc[0] and dup_null.iloc[j]==False:
                            if column in keep_all and keep_null==True:
                                final_keep_list.append([index_one,index_two])
                            elif column in keep_all:
                                continue
                            else:
                                duplicated_entries.loc[0,column] = duplicated_entries.loc[j,column]
                                #excel_sheet.loc[index_one,column] = duplicated_entries.loc[j,column]
                            #print(duplicated_entries[index_columns[0]][0],'replaced with=',duplicated_entries.loc[j,column])
                        #case 2 first entry is not null and second is  null: set entry 2 = entry 1
                        elif dup_null.iloc[0]==False and dup_null.iloc[j]:
                            if column in keep_all and keep_null==True:
                                final_keep_list.append([index_one,index_two])
                            elif column in keep_all:
                                continue
                            else:
                                #excel_sheet.loc[index_two,column] = duplicated_entries.loc[0,column]
                                duplicated_entries.loc[j,column] = duplicated_entries.loc[0 ,column]
                            #print(duplicated_entries[index_columns[0]][j],'replaced with=',duplicated_entries.loc[0,column])
                        #case 3 neither entry is null and values don't match: prompt user review of data enties
                        elif dup_null.iloc[0]==False and dup_null.iloc[j]==False and str(duplicated_entries[column][j]).lower()!= str(duplicated_entries[column][0]).lower():
                            
                            if column in combine_data:
                                data_list_0 = str(duplicated_entries.loc[0,column])
                                data_list_0=data_list_0.split(', ')
                                data_list_1 = str(duplicated_entries.loc[j,column])
                                data_list_1=data_list_1.split(', ')
                                data_list = set(data_list_0+data_list_1)
                                data_list = ['{0}'.format(data_piece) for data_piece in data_list]
                                data_str = ', '.join(data_list)
                                duplicated_entries.loc[0,column] = data_str
                                duplicated_entries.loc[j,column] = data_str
                            elif column in keep_all:
                                final_keep_list.append([index_one,index_two])
                            elif column in select_latest:
                                new_older_data = duplicated_entries.loc[j,column]
                                if duplicated_entries['Old '+column].isnull().iloc[0] == False:
                                    older_list = str(duplicated_entries.loc[0,'Old '+column])
                                    older_list = older_list.split(', ')
                                    if older_list == ['']:
                                        older_list=[]
                                else:
                                    older_list=[]
                                older_list.append(new_older_data)
                                older_list = ['{0}'.format(data_piece) for data_piece in older_list]
                                older_list = set(older_list)
                                older_str = ', '.join(older_list)
                                duplicated_entries.loc[0,'Old '+column] = older_str
                                duplicated_entries.loc[j,'Old '+column] = older_str
                                
                                duplicated_entries.loc[j,column] = duplicated_entries.loc[0,column]
                            elif (column not in select_latest and SubDateInc) or (SubDateInc == False):
                                #below prompts user for manual review in case of cintradicting data
                                if SubDateInc:
                                    print(' Submission Dates =',duplicated_entries['Submission Date'][0],',',duplicated_entries['Submission Date'][j])
                                print(duplicated_entries['Address/Cross Streets'][0],' | ',duplicated_entries['Address/Cross Streets'][j])
                                print(column)
                                print(duplicated_entries['Violent Incidents: Violent Incident #'][0])
                                print(duplicated_entries[index_columns[0]][0])
                                print('a =',duplicated_entries[column][0])
                                print('b =',duplicated_entries[column][j])
                                print('error')
                                #print('both')
                                response = manual_data_review()
                                if response == 'a':
                                    if duplicated_entries['Transformations to Data'].isnull().iloc[0] == False:
                                        transformation_list_0 = str(duplicated_entries.loc[0,'Transformations to Data'])
                                        transformation_list_0=transformation_list_0.split(',')
                                    else:
                                        transformation_list_0=[]
                                    if duplicated_entries['Transformations to Data'].isnull().iloc[j] == False:
                                        transformation_list_j = str(duplicated_entries.loc[j,'Transformations to Data'])
                                        transformation_list_j.split(',')
                                    else:
                                        transformation_list_j=[]
                                        
                                    transformation_list_j = ['{0}'.format(data_piece) for data_piece in transformation_list_j]
                                    transformation_list_0 = ['{0}'.format(data_piece) for data_piece in transformation_list_0]
                                    transformation_str = ','.join(set(transformation_list_0+transformation_list_j))
                                    new_transformation_data = column+': (selected: '+str(duplicated_entries[column][0])+' | discarded: '+str(duplicated_entries[column][j])+')'
                                    if transformation_str!='':
                                        transformation_str += ', '
                                    if new_transformation_data not in transformation_str:
                                        duplicated_entries.loc[0,'Transformations to Data'] = transformation_str+ new_transformation_data
                                        duplicated_entries.loc[j,'Transformations to Data'] = transformation_str+ new_transformation_data
                                    
                                    duplicated_entries.loc[j,column] = duplicated_entries.loc[0 ,column]
                                if response == 'b':
                                    #excel_sheet.loc[index_one,column] = duplicated_entries.loc[j,column]
                                    if duplicated_entries['Transformations to Data'].isnull().iloc[0] == False:
                                        transformation_list_0 = str(duplicated_entries.loc[0,'Transformations to Data'])
                                        transformation_list_0=transformation_list_0.split(',')
                                    else:
                                        transformation_list_0=[]
                                    if duplicated_entries['Transformations to Data'].isnull().iloc[j] == False:
                                        transformation_list_j = str(duplicated_entries.loc[j,'Transformations to Data'])
                                        transformation_list_j=transformation_list_j.split(',')
                                    else:
                                        transformation_list_j=[]
                                    transformation_list_j = ['{0}'.format(data_piece) for data_piece in transformation_list_j]
                                    transformation_list_0 = ['{0}'.format(data_piece) for data_piece in transformation_list_0]
                                    transformation_str = ','.join(set(transformation_list_0+transformation_list_j))
                                    new_transformation_data = column+': (selected: '+str(duplicated_entries[column][j])+' | discarded: '+str(duplicated_entries[column][0])+')'
                                    
                                    if new_transformation_data not in transformation_str:
                                        transformation_str += ', '
                                        duplicated_entries.loc[0,'Transformations to Data'] = transformation_str+ new_transformation_data
                                        duplicated_entries.loc[j,'Transformations to Data'] = transformation_str+ new_transformation_data
                                    
                                    duplicated_entries.loc[0,column] = duplicated_entries.loc[j,column]
                                elif response == 'error':
                                    Error = True
                                    break
                                #elif response == 'both':
                                #    final_keep_list.append([index_one,index_two])
                    #sets the row within the excel_sheet df equal to the row that has been being transformed in duplicate entries
                    excel_sheet.loc[index_two] = duplicated_entries.loc[j]
                excel_sheet.loc[index_one] = duplicated_entries.loc[0]
                excel_sheet.loc[index_one,'Total Victims']=total_vict
                append_final_list.append(index_one)
            else:
                append_final_list.append(index_one)
            
            #cur.execute(f'DELETE FROM excel_table WHERE {index_str}')
            #conn.commit() 
            #print(pandas.read_sql_query(f'SELECT COUNT(*) FROM excel_table',disk_engine))
    #conn.close()
    
    #print(final_keep_list)
    #runs through final_keep_ list, which contains rows kept automatically via keep_all and rows kept by user review 
    # it copies data from the first instance of the duplicate into any possible empty values (to account for rows further down the list providing additional data into the 1st instance)
    for column in excel_sheet.columns:
                if column not in final_df.columns:
                    final_df[column]=None
    final_data_share_cols = list(set(old_cols+column_names))
    final_data_share_cols.append('Transformations to Data')
    for i in final_keep_list:
        for column in final_data_share_cols:
            prime_value = excel_sheet.loc[i[0],column]
            dup_null = excel_sheet[column].isnull()
            if dup_null.iloc[i[0]]==False and column not in keep_all:
                excel_sheet.loc[i[1],column] = prime_value
        append_final_list.append(i[1])
        
    #transformation into sets remove any duplicate entries of indices
    append_final_list = set(append_final_list)
    append_error_list = set(append_error_list)
    #append_final_list = set(list(append_final_list)+append_null_list)
    append_final_list = append_final_list.difference(append_error_list)
    #above removes rows marked as errors from the final list
    

    for i in append_error_list:   
        new_entry = excel_sheet.iloc[i]
        error_sheet.loc[len(error_sheet)] = new_entry
    
    for i in append_final_list:   
        new_entry = excel_sheet.iloc[i]
        final_df.loc[len(final_df)] = new_entry
        
        
    
    final_drop = []
    kept_index_combin = list(set(index_columns+keep_all+column_names))
    final_duplicate_check = final_df.duplicated(keep='first',subset=kept_index_combin)
    for i in range(len(final_duplicate_check)):
        if final_duplicate_check[i]==True:
            final_drop.append(i)
            #final check for duplicate rows based on all columns, poetentially extraneous but I'd rather be cautious
            

    final_df.drop(final_drop,inplace=True) 
    final_df.reset_index(drop=True,inplace = True)
    
    #add null values to final_df
    for i in append_null_list:   
        new_entry = excel_sheet.iloc[i]
        final_df.loc[len(final_df)] = new_entry
        #print(len(final_df))
    
    #final_df.reset_index(drop=True,inplace = True)
    #reverts index columns that are dates back to format recognised as a date in excel
    for column in final_df.columns:
        if 'date' in column.lower():
            if column in index_columns:
                date_format3 = pandas.to_datetime(final_df[column], errors='coerce', format='%Y-%m-%d %H:%M:%S:%f')
                final_df[column] = date_format3
    for column in error_sheet.columns:
        if 'date' in column.lower():
            if column in index_columns:
                date_format4 = pandas.to_datetime(error_sheet[column], errors='coerce', format='%Y-%m-%d %H:%M:%S:%f')
                error_sheet[column] = date_format4
    
    #final_df.drop(columns='Age of victim', inplace=True)
    #todays_date = str(date.today())
    
    #excel_sheet.to_excel('DUPLICATES_'+file, index=True) 
    return final_df,error_sheet



def column_comparison(new_df,recent_compilation,lower_thresh=70):
    """detects columns with similiar names using fuzz, then asks user if the columns contain the same data
    if yes, it replaces thew new df's col name with the older, so they will combine correctly
    when the dfs are concatenated"""

    initial_drop_cols1=[]
    initial_drop_cols2=[]
    
    for column_name in new_df.columns:
        if new_df[column_name].isnull().all():
            initial_drop_cols1.append(column_name)
        elif 'Unnamed' in column_name:
            initial_drop_cols1.append(column_name)
    new_df.drop(columns=initial_drop_cols1,inplace=True)
    
    
    for column_name in recent_compilation.columns:
        if recent_compilation[column_name].isnull().all():
            initial_drop_cols2.append(column_name)
        elif 'Unnamed' in column_name:
            initial_drop_cols2.append(column_name)
    recent_compilation.drop(columns=initial_drop_cols2,inplace=True)
    
    col_names_dict = {}
    column_names_new_df = list(new_df.columns)
    #print(column_names_new_df)
    column_names_recent_compilation = list(recent_compilation.columns)
    for i in new_df:
        #populates dictionary with pairs of columns to themselves ie 'ID':'ID'
        col_names_dict[i]=i
    #print(col_names_dict)
        
   
   #goal: change the columns headers of the old compilation to match the new data
    
    for column_name1 in column_names_recent_compilation:
        if 'old' in column_name1.lower():
            continue
        if column_name1 in column_names_new_df:
            column_names_new_df.remove(column_name1)
        for column_name2 in column_names_new_df:
            l_d = fuzz.ratio(column_name1.lower(), column_name2.lower()) 
            if lower_thresh<l_d<100:
                print('matched cols=')
                print('old df = ',column_name1)
                print('new df = ',column_name2)
                print(l_d)
                print('replace / pass')
                response = input()
                if  response == 'replace':
                    col_names_dict[column_name2] = column_name1
    return new_df.rename(columns=col_names_dict)



def new_data_compile_ORGANIZATION_1(folder,keyword=[]):
    """searches through the specified workbook for sheets with titles containing the keyword, then returns the data within those sheets into 1 database"""
    current_date = datetime.datetime.now()
    excl_list=[]
    for i in keyword:
        file_list = glob.glob(folder+'/*'+i+'*.xlsx')
    #ORGANIZATION_1 path = 'C:\\Users\\HansenMade\\Python Files\\ORGANIZATION_1\\
        for file in file_list:
            read_file=pandas.read_excel(file)
            if read_file.empty==False:
                read_file = add_submission_date(current_date,read_file)
                read_file['Organization'] = 'ORGANIZATION_1'
                read_file['Form'] = i
                read_file['file origin'] = file
                #above adds a column to each dataframe containing a 'submission date' based off the name of the folder the excel file was in, comment out if undesired
                excl_list.append(read_file)
            
        
    if len(excl_list)!=0:
        excl_merged = pandas.concat(excl_list, ignore_index=True)
    else:
        excl_merged = pandas.DataFrame()
    #below is for test purposes, comment out later
    #excl_merged.to_excel('ORGANIZATION_1_'+keyword+'.xlsx', index=False)
    return excl_merged
 
def add_lat_long(file,address_column):
    incidents = pandas.read_excel(file)
    if 'Latitude' not in incidents.columns:
        incidents['Latitude'] = None
    if 'Longitude' not in incidents.columns:
        incidents['Longitude'] = None
    geolocator = Nominatim(user_agent="measurements",timeout = 10)
    null_list_address = incidents[address_column].isnull()
    null_list_lat = incidents['Latitude'].isnull()
    null_list_long = incidents['Longitude'].isnull()
    for row in range(len(incidents)):
        #for row in range(20):
        if null_list_address[row] == False and null_list_lat[row] == True and null_list_long[row] == True:
            cleaned_address, seperator, additional_information = incidents[address_column][row].partition('(')
            cleaned_address.replace(".", "")
            #print(incidents['Address/Cross streets'][row]+' Chicago IL')
            try:
                location = geolocator.geocode(cleaned_address + ' Chicago IL')
                #print(location.address)
                #print((location.latitude, location.longitude))
                incidents.loc[row,'Latitude'] = location.latitude
                incidents.loc[row,'Longitude'] = location.longitude
            except:
                continue
    incidents.to_excel(file,index = False)
	


def add_new_data(new_folder,recent_compilation_file,keyword=[],index_columns=[],select_latest=[],keep_all=[],keep_null=False,l_d_check = [],ignore=['sheetname','Submission Date','Organization','Form'],sort_by='Submission Date',sort_dir=False,combine_data=['file origin']):
    """takes in new data from specified file, combines this new data and the current compiled data. it then checks for duplicates
    it results in 1 new combined cleaned excel sheet, 1 new error sheet, and a duplicate of the former data compilation for record keeping"""
    current_date = datetime.datetime.now()
    if current_date.month!=1:
        last_mo=str(current_date.month-1)+'-01-'+str(current_date.year)
    else:
        last_mo='12-01-'+str(current_date.year-1)
    new_df = new_data_compile_ORGANIZATION_1(new_folder,keyword)
    if new_df.empty==True:
        print(keyword,'No new data')
        pause=input()
        return
    for column in new_df.columns:
        if 'date' in column.lower():
            date_format1 = pandas.to_datetime(new_df[column], errors='coerce', format='%m/%d/%Y')
            if column in index_columns:
                for i in range(len(date_format1)):
                    if type(date_format1[i]) != pandas._libs.tslibs.nattype.NaTType:
                        date_format1[i] = date_format1[i].strftime('%Y-%m-%d %H:%M:%S:%f')
                        #print(i)
            date_format2 = pandas.to_datetime(new_df[column], errors='coerce', format='%Y-%m-%d %H:%M:%S')
            if column in index_columns:
                for i in range(len(date_format2)):
                    if type(date_format2[i]) != pandas._libs.tslibs.nattype.NaTType:
                        date_format2[i] = date_format2[i].strftime('%Y-%m-%d %H:%M:%S:%f')
            new_df[column] = date_format1.fillna(date_format2)
    
    recent_compilation = pandas.read_excel(recent_compilation_file)
    #below creates a dated copy of recent dompilation before adding new data for recrd keeping
    file_name = recent_compilation_file.rsplit('\\',1)[1]
    recent_compilation.to_excel('C:\\Users\\HansenMade\\Python Files\\ORGANIZATION_1 Compiled Data\\old vers of compilations\\'+last_mo+'_'+file_name, index=False)
    if 'Age of victim' in recent_compilation.columns:
        recent_compilation.rename(columns = {'Age of victim':'Age of Victim 1'},inplace=True)
    if 'Age of victim' in new_df.columns:
        new_df.rename(columns = {'Age of victim':'Age of Victim 1'},inplace=True)
        #do header comparison, with headers for the compilation changing to match new ones (ie if old one is Case ID and new is Case Record ID, the old one with be chnaged to the new one)
    #print(new_df.columns)
    #print(recent_compilation.columns)
    #return
    new_df = column_comparison(new_df,recent_compilation)
    #print(new_df)
    new_df = new_df.loc[new_df['Date of Incident  ↑'] >= pandas.Timestamp(last_mo)]
    
    excel_sheet = pandas.concat([new_df,recent_compilation], ignore_index=True)
    dfs = combine_duplicate_row(excel_sheet, index_columns = index_columns,select_latest=select_latest,keep_all=keep_all,keep_null=keep_null,l_d_check = l_d_check,ignore=ignore,sort_by=sort_by,sort_dir=sort_dir,combine_data=combine_data)
    #print(dfs)
    final_df = dfs[0]
    error_df = dfs[1]
    
    todays_date = datetime.date.today()
    
    error_df.to_excel('C:\\Users\\HansenMade\\Python Files\\ORGANIZATION_1 Compiled Data\\error sheets\\error_'+str(todays_date)+'_'+file_name, index=False)
    final_df.to_excel('C:\\Users\\HansenMade\\Python Files\\ORGANIZATION_1 Compiled Data\\cleaned\\ORGANIZATION_1_incident_by_incident.xlsx', index=False)
    
#have location be select_latest
#have vilent incident # be combine


add_new_data("C:\\Users\\HansenMade\\Python Files\\ORGANIZATION_1 Compiled Data\\ORGANIZATION_1 New Data","C:\\Users\\HansenMade\\Python Files\\ORGANIZATION_1 Compiled Data\\cleaned\\ORGANIZATION_1_incident_by_incident.xlsx",keyword = ['incident'], index_columns = ['Date of Incident  ↑','Address/Cross Streets'],select_latest=['Zip Code','Could there be retaliation for incident?','Submission Date'],keep_all=[],keep_null=False,l_d_check = [],ignore=['file origin','sheetname','Organization','Form'],sort_by='Submission Date',sort_dir=False,combine_data=['Was it a retaliation?','Who notified staff?','Reason for incident','Staff entering report','Violent Incidents: Record Type  ↑','Violent Incidents: Violent Incident #','file origin'])

