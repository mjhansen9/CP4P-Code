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
import datetime
from datetime import datetime, date, timedelta
import numpy as np

warnings.simplefilter("ignore", category=FutureWarning)    
pandas.options.mode.chained_assignment = None  
"""Scorecard generation
goal generate scorecard data for Brooke (friend)
variables needed:

OW Part Info:
Participant ID
Form
Organization
Participant assigned case manager
Participant start date
OW or CM participant? (OW/CM)
Participant community
Participant age
Participant gender
Participant ethnicity
Participant race

CM Part Info:
Participant ID
Form
Organization
Participant assigned case manager
Participant start date
OW or CM participant? (OW/CM)
Moved from outreach to case management? (Y/N)
Participant community
Participant age
Participant gender
Participant ethnicity
Participant race

CM case notes:
Participant ID
Form
Organization
Participant contact staff member
Participant contact date
Participant contact time

OW casenotes:
Participant ID
Form
Organization
Participant contact staff member
Participant contact date

Case management participant referrals (connections) to services:
Participant ID
Form
Organization
Participant referral staff
Participant referral date
Participant referral type (Education, Employment, Food assistance, Health, Housing, Identification documents, Legal, Mental health, Substance use)


Risk Reduction Form:
Just the whole thang

Incident responses:
Form
Organization
Staff member(s) that contributed to the incident response
Incident notification date/time
Incident response date/time
CPIC address
Incident address

"""

def staff_search(df,col_basis,date_col=None,org = None):
    """takes in a df, and searches a compiled staff listing for the each row's associated staff name. determines if staff member is a funded worker in a certain time period. removes unfunded workers"""
    
    #df.to_excel('PRE-TEST_CM_OW.xlsx', index=False)
    staff_list = pandas.read_excel("C:\\Users\\HansenMade\\Downloads\\000-Staff Listing New 06.05.24(13).xlsx",sheet_name = 'FY25 July-Dec')
    if org!=None:
        staff_list = staff_list[staff_list['Provider'].str.contains(org, na=False)]
    #print(staff_list)
    staff_list.replace('%', inplace=True)
    staff_list.fillna(0,inplace=True)
    #staff_list.to_excel("test.xlsx")
    column_list = staff_list.columns
    date_col_list=column_list[8:18]
    filtered_col_dfs=[]
    for i in date_col_list:
        staff_list[i] = staff_list[i]*100
        staff_list[i].astype(int)
        new_df = staff_list[['Staff Name',i,'Title']].copy()
        new_df = new_df[new_df[i] != 0]
        new_df.reset_index(drop=True,inplace=True)
        filtered_col_dfs.append(new_df)
        #new_df.rename(columns={i:i.strftime('%Y-%m-%d %H:%M:%S:%f')},inplace=True)
        
    if date_col != None:
        
        date_format1 = pandas.to_datetime(df[date_col], errors='coerce', format='%Y-%m-%d')
        for i in range(len(date_format1)):
            if type(date_format1[i]) != pandas._libs.tslibs.nattype.NaTType:
                date_format1[i] = date_format1[i].strftime('%Y-%m-%d %H:%M:%S:%f')
                    #print(i)
        date_format2 = pandas.to_datetime(df[date_col], errors='coerce', format='%Y-%m-%d %H:%M:%S')
        for i in range(len(date_format2)):
            if type(date_format2[i]) != pandas._libs.tslibs.nattype.NaTType:
                date_format2[i] = date_format2[i].strftime('%Y-%m-%d %H:%M:%S:%f')
        date_format3 = pandas.to_datetime(df[date_col], errors='coerce', format='%m/%d/%Y')
        for i in range(len(date_format3)):
            if type(date_format3[i]) != pandas._libs.tslibs.nattype.NaTType:
                date_format3[i] = date_format3[i].strftime('%Y-%m-%d %H:%M:%S:%f')
        date_format4 = pandas.to_datetime(df[date_col], errors='coerce', format='%m/%d/%Y %H:%M:%S')
        for i in range(len(date_format4)):
            if type(date_format4[i]) != pandas._libs.tslibs.nattype.NaTType:
                date_format4[i] = date_format4[i].strftime('%Y-%m-%d %H:%M:%S:%f')    
    
        df_date_ranges = []
        for i in range(len(date_col_list)):
            disk_engine = create_engine('sqlite:///my_lite_store.db')
            df.to_sql('ow_cm_parts_table', disk_engine, if_exists='replace',index=False)
            #print(date_col_list[i])
            end_date = date_col_list[i].strftime('%Y-%m-%d %H:%M:%S:%f')
            if i==0:
                start_date = None
            else:
                start_date = date_col_list[i-1].strftime('%Y-%m-%d %H:%M:%S:%f')
                
            if start_date==None:
                index_str = '"'+ date_col+'" <= "'+str(end_date)+'"'
            else:
                index_str = '"'+ date_col+'" >= "'+str(start_date)+'" AND "'+date_col+'" <= "'+str(end_date)+'"'
            df_final = pandas.read_sql_query(f'SELECT * FROM ow_cm_parts_table WHERE {index_str}',disk_engine)
            df_date_ranges.append(df_final)
            
            
            
            unique_staff_names_TF = df_final.duplicated(keep='first',subset=col_basis)
            #df_final.to_excel('TEST_'+str(i)+'.xlsx', index=False)
            #below loops over list of unique staff names in given time frame
            for df_row in range(len(unique_staff_names_TF)):
                if unique_staff_names_TF[df_row]==False:
                    assignment = None
                    #print(df_final[col_basis][df_row])
                     #If value is false then it means it is either unique or the first occurance of a duplicate
                    if type(df_final[col_basis][df_row])==str:
                        if '_' in df_final[col_basis][df_row]:
                            name_split = df_final[col_basis][df_row].split('_')
                            staff_name = name_split[1]+', '+name_split[0]
                        elif '.' in df_final[col_basis][df_row]:
                            name_split = df_final[col_basis][df_row].split('.')
                            staff_name = name_split[1]+', '+name_split[0]
                        elif ',' not in df_final[col_basis][df_row]:
                            try:
                                name_split = df_final[col_basis][df_row].split(' ')
                                staff_name = name_split[1]+', '+name_split[0]
                            except:
                                staff_name = df_final[col_basis][df_row]
                        else:
                            staff_name = df_final[col_basis][df_row]
                    else:
                        continue
                    organization_staff = filtered_col_dfs[i]
                    #print(organization_staff['Staff Name'
                    for staff_list in range(len(organization_staff)):
                        staff_roles = None
                        staff_name_from_listing = organization_staff['Staff Name'][staff_list]
                        #loop over organization_staff and do an l_d check for similarity
                        #need to account for possibility that names are like this 'LN, FN' or like this 'FN LN' as those dont result in high 
                        #l_d even if it is the same name. Seperate by comma
                        
                            
                        l_d = fuzz.ratio(staff_name.lower(),staff_name_from_listing.lower() )
                        if l_d>90:
                            
                            #print(staff_name)
                            #print(staff_name_from_listing)
                            #print(l_d)
                            #90 feels good
                            #need to mark every column with that worker as 'yes'
                            
                            staff_roles = str(organization_staff['Title'][staff_list]).lower()
                        if staff_roles!=None:
                            if 'case manager' in staff_roles:
                                assignment = 'Case Management'
                            elif 'outreach' in staff_roles:
                                assignment = 'Outreach'
                        if assignment!=None:
                            df_final['Case Management/Outreach'][df_final[col_basis]==df_final[col_basis][df_row]] = assignment
                            break
            
        filtered_staff_df = pandas.concat(df_date_ranges, ignore_index=True)
    else:
        df_final = df.copy()
        unique_staff_names_TF = df_final.duplicated(keep='first',subset=col_basis)
        #data does not provdie specific date values, concat filtered_col_dfs then run check against that
        organization_staff = pandas.concat(filtered_col_dfs, ignore_index=True)
        # drop duplicate staff
        organization_staff.drop_duplicates(subset=['Staff Name'],inplace=True,ignore_index=True)
        for df_row in range(len(unique_staff_names_TF)):
                if unique_staff_names_TF[df_row]==False:
                    assignment = None
                    #print(df_final[col_basis][df_row])
                     #If value is false then it means it is either unique or the first occurance of a duplicate
                    if type(df_final[col_basis][df_row])==str:
                        if '_' in df_final[col_basis][df_row]:
                            name_split = df_final[col_basis][df_row].split('_')
                            staff_name = name_split[1]+', '+name_split[0]
                        elif '.' in df_final[col_basis][df_row]:
                            name_split = df_final[col_basis][df_row].split('.')
                            staff_name = name_split[1]+', '+name_split[0]
                        elif ',' not in df_final[col_basis][df_row]:
                            try:
                                name_split = df_final[col_basis][df_row].split(' ')
                                staff_name = name_split[1]+', '+name_split[0]
                            except:
                                staff_name = df_final[col_basis][df_row]
                        else:
                            staff_name = df_final[col_basis][df_row]
                    else:
                        continue
                    #print(organization_staff['Staff Name'
                    for staff_list in range(len(organization_staff)):
                        staff_roles = None
                        staff_name_from_listing = organization_staff['Staff Name'][staff_list]
                        #loop over organization_staff and do an l_d check for similarity
                        #need to account for possibility that names are like this 'LN, FN' or like this 'FN LN' as those dont result in high 
                        #l_d even if it is the same name. Seperate by comma
                        
                            
                        l_d = fuzz.ratio(staff_name.lower(),staff_name_from_listing.lower() )
                        if l_d>90:
                            
                            #print(staff_name)
                            #print(staff_name_from_listing)
                            #print(l_d)
                            #90 feels good
                            #need to mark every column with that worker as 'yes'
                            
                            staff_roles = str(organization_staff['Title'][staff_list]).lower()
                        if staff_roles!=None:
                            if 'case manager' in staff_roles:
                                assignment = 'Case Management'
                            elif 'outreach' in staff_roles:
                                assignment = 'Outreach'
                        if assignment!=None:
                            df_final['Case Management/Outreach'][df_final[col_basis]==df_final[col_basis][df_row]] = assignment
                            break
                        
        filtered_staff_df = df_final
     
    
            
                    
    #filtered_staff_df.to_excel('TEST_CM_OW.xlsx', index=False)
    return filtered_staff_df

def transform_data(file_df=None,file=None,str_combine={},num_add={},num_sub={},col_div={},drop_list=[],date_convers=False):
    """converts data to match format that redcap data comes in in the following ways:
    a) it converts any datetimes to standard m/d/y formats
    b) it allows for the combination of string columns (with option to drop of keep either of the cols)
    c) it allows for the addition/subtraction of numerical cols
    d) it allows for divison on numerical cols
    """
    
    #format for str_combine and num_add assumed to be like this {'Canvassing (hours)':['Hours spent conducting conflict mediations:']} to allow for combination of 2+ cols
    #col_div assumed {'Canvassing (minutes)':60}
    if file!=None:
        file_df = pandas.read_excel(file)
    if date_convers==True:
        for i in file_df.columns:
            if 'date' in i.lower():
                file_df[i] = file_df[i].dt.strftime('%m/%d/%Y')
    #print(file_df) 
    
    for dict_key in str_combine:
        for list_item in str_combine[dict_key]:
            file_df[dict_key] = file_df[dict_key].astype(str) + ' - ' +file_df[list_item].astype(str)
        #this woiks
    
    for i in num_add:
        file_df[i] = file_df[i].astype(float)
        for list_item in num_add[i]:
            #print(list_item)
            file_df[i] = file_df[i].add(file_df[list_item].astype(float))

    for i in num_sub:
        file_df[i] = file_df[i].astype(float)
        for list_item in num_sub[i]:
            file_df[i] = file_df[i].subtract(file_df[list_item].astype(float))
    
    for i in col_div:
        file_df[i] = file_df[i].astype(float)
        file_df[i] = file_df[i].div(col_div[i])
        #this is woiking

    file_df.drop(columns = drop_list,inplace=True)
    
    
    
    #print(file_name)
    if file==None:
        return file_df
    else:
        file_name = file.rsplit('\\',1)[1]
        file_df.to_excel("transformed_"+file_name, index=False)
    


def rc_nonrc_merge(start_date=None,end_date=None):
    if start_date!=None:
        start = datetime.strptime(start_date,'%Y-%m-%d')
        start = start.strftime('%Y-%m-%d %H:%M:%S:%f')
    if end_date!=None:
        end = datetime.strptime(end_date,'%Y-%m-%d')
        end = end.strftime('%Y-%m-%d %H:%M:%S:%f')
        
    final_dfs = []
    
    
    #OW&CM Part info
    #needed cols:
    """Participant ID
    Form
    Organization
    Participant assigned case manager
    Participant start date
    OW or CM participant? (OW/CM)
    Participant community
    Participant age
    Participant gender
    Participant ethnicity
    Participant race"""

    ow_cm_part_dfs=[]
    
    #ORGANIZATION_1
    
    BT_ow_cm_part_df_intake = pandas.read_excel("C:\\Users\\HansenMade\\Python Files\\ORGANIZATION_1 Compiled Data\\cleaned\\ORGANIZATION_1_intake_dismissal_dedup.xlsx")
    BT_ow_cm_part_df = pandas.read_excel("C:\\Users\\HansenMade\\Python Files\\ORGANIZATION_1 Compiled Data\\cleaned\\ORGANIZATION_1_prog_note.xlsx")
    #['Data Access Group','Community of residence','Record ID','Enrolled Date','Case Management/Outreach','Date of Activity']
    
    
    #['Data Access Group','Community of residence','Record ID','Enrolled Date','Case Management/Outreach','Date of Activity']
    
    BT_ow_cm_part_df_intake.rename(columns={'Enrolled Date':'Participant start date','Owner: Full Name':'Owner Name','Program Name':'Case Management/Outreach'},inplace=True)
    BT_ow_cm_part_df.rename(columns={'Progress Note: Created Date':'Date of Activity'},inplace=True)
    
    #intake_dismissal_dates = BT_ow_cm_part_df_intake[["Case Record ID",'Participant start date',"Dismissal Date"]]
    
    needed_cols=['Age','Case-Safe Contact ID','Gender','Client Race','Ethnicity','Status','Participant start date',"Dismissal Date",'Owner Name','Case Management/Outreach']
    dropped_cols=[]
    for i in BT_ow_cm_part_df_intake.columns:
        if i  not in needed_cols:
            dropped_cols.append(i)
    BT_ow_cm_part_df_intake.drop(columns=dropped_cols,inplace=True)
    BT_ow_cm_part_df_intake.reset_index(drop=True,inplace=True)
    
    
    
    needed_cols=['Case Record ID','Data Access Group','Case Management/Outreach','Date of Activity','Case-Safe Contact ID','Progress Note: Created By']
    dropped_cols=[]
    for i in BT_ow_cm_part_df.columns:
        if i  not in needed_cols:
            dropped_cols.append(i)
    BT_ow_cm_part_df.drop(columns=dropped_cols,inplace=True)
    BT_ow_cm_part_df.reset_index(drop=True,inplace=True)
    
    
    
    
    date_format1 = pandas.to_datetime(BT_ow_cm_part_df['Date of Activity'], errors='coerce', format='%Y-%m-%d')
    for i in range(len(date_format1)):
        if type(date_format1[i]) != pandas._libs.tslibs.nattype.NaTType:
            date_format1[i] = date_format1[i].strftime('%Y-%m-%d %H:%M:%S:%f')
                #print(i)
    date_format2 = pandas.to_datetime(BT_ow_cm_part_df['Date of Activity'], errors='coerce', format='%Y-%m-%d %H:%M:%S')
    for i in range(len(date_format2)):
        if type(date_format2[i]) != pandas._libs.tslibs.nattype.NaTType:
            date_format2[i] = date_format2[i].strftime('%Y-%m-%d %H:%M:%S:%f')
    date_format3 = pandas.to_datetime(BT_ow_cm_part_df['Date of Activity'], errors='coerce', format='%m/%d/%Y')
    for i in range(len(date_format3)):
        if type(date_format3[i]) != pandas._libs.tslibs.nattype.NaTType:
            date_format3[i] = date_format3[i].strftime('%Y-%m-%d %H:%M:%S:%f')
    date_format4 = pandas.to_datetime(BT_ow_cm_part_df['Date of Activity'], errors='coerce', format='%m/%d/%Y %H:%M:%S')
    for i in range(len(date_format4)):
        if type(date_format4[i]) != pandas._libs.tslibs.nattype.NaTType:
            date_format4[i] = date_format4[i].strftime('%Y-%m-%d %H:%M:%S:%f')
            
    date_format1 = date_format1.fillna(date_format2)
    date_format1 = date_format1.fillna(date_format3)
    BT_ow_cm_part_df['Date of Activity'] = date_format1.fillna(date_format4)  
    #will need to adjust for additional date formats as they come
    
    if start_date!=None or end_date!=None:
        disk_engine = create_engine('sqlite:///my_lite_store.db')
        BT_ow_cm_part_df.to_sql('ow_cm_parts_table', disk_engine, if_exists='replace',index=False)
        
        if start_date!=None and end_date!=None:
            index_str='"Date of Activity" >= '+'"'+str(start)+'"'+' AND "Date of Activity" <= '+'"'+str(end)+'"'
        elif start_date!=None:
            index_str='"Date of Activity" >= '+'"'+str(start)+'"'
        else:
            index_str='"Date of Activity" <= '+'"'+str(end)+'"'
        
        #print(index_str)
        BT_ow_cm_part_df_final = pandas.read_sql_query(f'SELECT * FROM ow_cm_parts_table WHERE {index_str}',disk_engine)
    else:
        BT_ow_cm_part_df_final = BT_ow_cm_part_df
   
    for column in BT_ow_cm_part_df_final.columns:
        if 'date' in column.lower():
            date_format3 = pandas.to_datetime(BT_ow_cm_part_df_final[column], errors='coerce', format='%Y-%m-%d %H:%M:%S:%f').dt.date
            BT_ow_cm_part_df_final[column] = date_format3
    
    #print(len(BT_ow_cm_part_df_final))
    
    BT_ow_cm_part_df_final=BT_ow_cm_part_df_final.merge(BT_ow_cm_part_df_intake,how='left',on='Case-Safe Contact ID')
    #print(len(BT_ow_cm_part_df_final))
    #BT_ow_cm_part_df_final=BT_ow_cm_part_df_final.merge(intake_dismissal_dates,how='left',on='Case Record ID')
    #print(len(BT_ow_cm_part_df_final))
    #print(intake_dismissal_dates)
    
    BT_ow_cm_part_df_final['Community of residence']= 'East Garfield Park'
    
    
    #run staff check
    #BT_ow_cm_part_df_final = staff_search(BT_ow_cm_part_df_final,'Owner Name',date_col=None,org='ORGANIZATION_1')
    #print(len(BT_ow_cm_part_df_final))
    #BT_ow_cm_part_df_final.to_excel('BT_PART_TEST_1.xlsx')
    BT_ow_cm_part_df_final.drop_duplicates(subset='Case-Safe Contact ID', keep='first', inplace=True, ignore_index=True)
    #sort by submission date (to not lose part that dont have an enrolled date)
    #BT_ow_cm_part_df_final.to_excel('BT_PART_TEST_2.xlsx')
    BT_ow_cm_part_df_final.reset_index(drop=True,inplace=True)
    
    BT_ow_cm_part_df_final.replace({'Violence Prevention':'Case Management','Violence Prevention Outreach':'Outreach'}, inplace = True)
    
    BT_ow_cm_part_df_final['Form'] = None
    
    BT_ow_cm_part_df_final['Form'][BT_ow_cm_part_df_final['Case Management/Outreach']=='Case Management'] = 'Case management participant demographics'
    BT_ow_cm_part_df_final['Form'][BT_ow_cm_part_df_final['Case Management/Outreach']=='Outreach'] = 'Outreach participant demographics'
    BT_ow_cm_part_df_final['Organization']='ORGANIZATION_1'
    
    BT_ow_cm_part_df_final['Participant assigned outreach worker'] = BT_ow_cm_part_df_final['Owner Name'].copy()
    
    BT_ow_cm_part_df_final['Participant assigned outreach worker'][BT_ow_cm_part_df_final['Case Management/Outreach']=='Case Management'] = None
    
    BT_ow_cm_part_df_final.rename(columns={'Owner Name':'Participant assigned case manager'},inplace=True)
    
    BT_ow_cm_part_df_final['Participant assigned case manager'][BT_ow_cm_part_df_final['Case Management/Outreach']=='Outreach'] = None
    
    BT_ow_cm_part_df_final['Moved from outreach to case management? (Y/N)'] = 'N/A'
    
    BT_ow_cm_part_df_final.rename(columns={'Case-Safe Contact ID':'Participant ID',
                                            'Case Management/Outreach':'OW or CM participant? (OW/CM)',
                                            'Community of residence':'Participant community',
                                            'Age':'Participant age',
                                            'Gender':'Participant gender',
                                            'Ethnicity':'Participant ethnicity',
                                            'Client Race':'Participant race'},inplace=True)
    
 
    
    
    
    ow_cm_part_dfs.append(BT_ow_cm_part_df_final)
    
    
    
    
    
    
    
    #ORGANIZATION_2
    En_cm_part_df = pandas.read_excel("C:\\Users\\HansenMade\\Python Files\\ORGANIZATION_2 compiled data\\Cleaned\\ORGANIZATION_2_participants - CM.xlsx")
    En_ow_part_df = pandas.read_excel("C:\\Users\\HansenMade\\Python Files\\ORGANIZATION_2 compiled data\\Cleaned\ORGANIZATION_2_participants - OW.xlsx")
    En_cmbc_part_df = pandas.read_excel("C:\\Users\\HansenMade\\Python Files\\ORGANIZATION_2 compiled data\\Cleaned\\StBC Caseload.xlsx")
    
    
    #change header names as needed and merge above dfs
    En_cm_part_df['Moved from outreach to case management? (Y/N)']=None
    En_cm_part_df.rename(columns={'Unique ID':'Participant ID','Intake Date':'Participant start date','Submission Date':'Date of Activity','Assigned to':'Participant assigned case manager','All - Age range':'Participant age','All - Gender':'Participant gender','All - Race / ethnicity':'Participant race'},inplace=True)
    En_cm_part_df['OW or CM participant? (OW/CM)']='Case Management'
    En_cm_part_df['Form'] = 'Case management participant demographics'
    En_cm_part_df['Moved from outreach to case management? (Y/N)'][En_cm_part_df['All - How was participant recruited / referred?'].str.contains('Outreach',na=False)]='Yes'
    #print(En_cm_part_df.columns)
    #En_cm_part_df.drop_duplicates(subset='Participant ID', keep='first', inplace=True, ignore_index=True)
    
    En_cmbc_part_df['Moved from outreach to case management? (Y/N)']=None
    En_cmbc_part_df.rename(columns={'Unique ID':'Participant ID','Intake Date':'Participant start date','Submission Date':'Date of Activity','Assigned to':'Participant assigned case manager','All - Age range':'Participant age','All - Gender':'Participant gender','All - Race / ethnicity':'Participant race'},inplace=True)
    En_cmbc_part_df['OW or CM participant? (OW/CM)']='Case Management'
    En_cmbc_part_df['Form'] = 'Case management participant demographics'
    En_cmbc_part_df['Moved from outreach to case management? (Y/N)'][En_cmbc_part_df['All - How was participant recruited / referred?'].str.contains('Outreach',na=False)]='Yes'
                           
    #print(En_cm_part_df.columns)
    #En_cmbc_part_df.drop_duplicates(subset='Participant ID', keep='first', inplace=True, ignore_index=True)
    En_cm_part_df = pandas.concat([En_cm_part_df,En_cmbc_part_df], ignore_index=True)
            
    
    En_ow_part_df.rename(columns={'Unique ID':'Participant ID','Intake Date':'Participant start date','Submission Date':'Date of Activity','Assigned to':'Participant assigned outreach worker','All - Age range':'Participant age','All - Gender':'Participant gender','All - Race / ethnicity':'Participant race'},inplace=True)
    En_ow_part_df['OW or CM participant? (OW/CM)']='Outreach'
    En_ow_part_df['Form'] = 'Outreach participant demographics'
    #En_ow_part_df.drop_duplicates(subset='Participant ID', keep='first', inplace=True, ignore_index=True)
    
    En_ow_cm_part_df = pandas.concat([En_cm_part_df,En_ow_part_df], ignore_index=True)
    En_ow_cm_part_df['Participant ethnicity'] = En_ow_cm_part_df['Participant race'].copy()
    En_ow_cm_part_df['Organization']='ORGANIZATION_2'
    En_ow_cm_part_df['Participant community']='South Lawndale'
    
    needed_cols=['Moved from outreach to case management? (Y/N)','Participant ID','Form','Organization','Participant assigned outreach worker','Participant assigned case manager','Participant start date','OW or CM participant? (OW/CM)','Participant community','Participant age','Participant gender','Participant ethnicity','Participant race','Date of Activity']
    dropped_cols=[]
    for i in En_ow_cm_part_df.columns:
        if i  not in needed_cols:
            dropped_cols.append(i)
    En_ow_cm_part_df.drop(columns=dropped_cols,inplace=True)
    En_ow_cm_part_df.reset_index(drop=True,inplace=True)
    
    if start_date!=None:
        start_ORGANIZATION_2 = datetime.strptime(start_date,'%Y-%m-%d') + timedelta(weeks = 4)
    if end_date!=None:
        end_ORGANIZATION_2 = datetime.strptime(end_date,'%Y-%m-%d') + timedelta(weeks = 4)
    date_format1 = pandas.to_datetime(En_ow_cm_part_df['Date of Activity'], errors='coerce', format='%Y-%m-%d')
    for i in range(len(date_format1)):
        if type(date_format1[i]) != pandas._libs.tslibs.nattype.NaTType:
            date_format1[i] = date_format1[i].strftime('%Y-%m-%d %H:%M:%S:%f')
                #print(i)
    date_format2 = pandas.to_datetime(En_ow_cm_part_df['Date of Activity'], errors='coerce', format='%Y-%m-%d %H:%M:%S')
    for i in range(len(date_format2)):
        if type(date_format2[i]) != pandas._libs.tslibs.nattype.NaTType:
            date_format2[i] = date_format2[i].strftime('%Y-%m-%d %H:%M:%S:%f')
    date_format3 = pandas.to_datetime(En_ow_cm_part_df['Date of Activity'], errors='coerce', format='%m/%d/%Y')
    for i in range(len(date_format3)):
        if type(date_format3[i]) != pandas._libs.tslibs.nattype.NaTType:
            date_format3[i] = date_format3[i].strftime('%Y-%m-%d %H:%M:%S:%f')
    date_format4 = pandas.to_datetime(En_ow_cm_part_df['Date of Activity'], errors='coerce', format='%m/%d/%Y %H:%M:%S')
    for i in range(len(date_format4)):
        if type(date_format4[i]) != pandas._libs.tslibs.nattype.NaTType:
            date_format4[i] = date_format4[i].strftime('%Y-%m-%d %H:%M:%S:%f')
            
    date_format1 = date_format1.fillna(date_format2)
    date_format1 = date_format1.fillna(date_format3)
    En_ow_cm_part_df['Date of Activity'] = date_format1.fillna(date_format4)  
    #will need to adjust for additional date formats as they come
    
    if start_date!=None or end_date!=None:
        disk_engine = create_engine('sqlite:///my_lite_store.db')
        En_ow_cm_part_df.to_sql('sql_table', disk_engine, if_exists='replace',index=False)
        
        if start_date!=None and end_date!=None:
            index_str='"Date of Activity" >= '+'"'+str(start_ORGANIZATION_2)+'"'+' AND "Date of Activity" <= '+'"'+str(end_ORGANIZATION_2)+'"'
        elif start_date!=None:
            index_str='"Date of Activity" >= '+'"'+str(start_ORGANIZATION_2)+'"'
        else:
            index_str='"Date of Activity" <= '+'"'+str(end_ORGANIZATION_2)+'"'
        
        #print(index_str)
        En_ow_cm_part_df_final = pandas.read_sql_query(f'SELECT * FROM sql_table WHERE {index_str}',disk_engine)
    else:
        En_ow_cm_part_df_final = En_ow_cm_part_df
    
    for column in En_ow_cm_part_df_final.columns:
        if 'date' in column.lower():
            date_format3 = pandas.to_datetime(En_ow_cm_part_df_final[column],errors='coerce' ,format='%Y-%m-%d %H:%M:%S.%f').dt.date
            En_ow_cm_part_df_final[column] = date_format3 
    
    En_ow_cm_part_df_final.drop_duplicates(subset='Participant ID', keep='first', inplace=True, ignore_index=True)
    
    #En_ow_cm_part_df_final.to_excel('EN_PART_TEST.xlsx')
    #return
    
    # as ORGANIZATION_2 pre-filters by funding there's no need to run that check on them
    ow_cm_part_dfs.append(En_ow_cm_part_df_final)

    needed_cols=['Moved from outreach to case management? (Y/N)','Participant ID','Form','Organization','Participant assigned outreach worker','Participant assigned case manager','Participant start date','OW or CM participant? (OW/CM)','Participant community','Participant age','Participant gender','Participant ethnicity','Participant race']
    
    for df in ow_cm_part_dfs:
        dropped_cols=[]
        for i in df.columns:
            if i  not in needed_cols:
                dropped_cols.append(i)
        df.drop(columns=dropped_cols,inplace=True)

    en_bt_parts = pandas.concat(ow_cm_part_dfs, ignore_index=True)
    final_dfs.append(en_bt_parts)
    #en_bt_parts.to_excel('TEST1.xlsx')
    
    
    #CM Case Notes
    
    """CM case notes:
    Participant ID
    Form
    Organization
    Participant contact staff member
    Participant contact date
    Participant contact time

    OW casenotes:
    Participant ID
    Form
    Organization
    Participant contact staff member
    Participant contact date
    Participant contact time
    """
    
    #ORGANIZATION_1
    case_notes=[]
    #just copy above but don't dedup 
    BT_ow_cm_part_df_intake = pandas.read_excel("C:\\Users\\HansenMade\\Python Files\\ORGANIZATION_1 Compiled Data\\cleaned\\ORGANIZATION_1_intake_dismissal_dedup.xlsx")
    BT_ow_cm_part_df = pandas.read_excel("C:\\Users\\HansenMade\\Python Files\\ORGANIZATION_1 Compiled Data\\cleaned\\ORGANIZATION_1_prog_note.xlsx")
    #['Organization','Participant community','Participant ID','Participant start date','OW or CM participant? (OW/CM)','Participant contact date']
    
    BT_ow_cm_part_df_intake.rename(columns={'Case-Safe Contact ID':'Case-safe ID (18 digits)','Owner: Full Name':'Participant assigned worker','Program Name':'Case Management/Outreach'},inplace=True)
    BT_ow_cm_part_df.rename(columns={'Progress Note: Created By':'Participant contact staff member','Progress Note: Created Date':'Participant contact date','Case-Safe Contact ID':'Case-safe ID (18 digits)'},inplace=True)
    
    
    
    needed_cols=['Case-safe ID (18 digits)','Participant assigned worker','Case Management/Outreach']
    dropped_cols=[]
    for i in BT_ow_cm_part_df_intake.columns:
        if i  not in needed_cols:
            dropped_cols.append(i)
    BT_ow_cm_part_df_intake.drop(columns=dropped_cols,inplace=True)
    BT_ow_cm_part_df_intake.reset_index(drop=True,inplace=True)
    
    needed_cols=['Organization','Participant contact staff member','Participant contact date','Case-safe ID (18 digits)']
    dropped_cols=[]
    for i in BT_ow_cm_part_df.columns:
        if i  not in needed_cols:
            dropped_cols.append(i)
    BT_ow_cm_part_df.drop(columns=dropped_cols,inplace=True)
    BT_ow_cm_part_df.reset_index(drop=True,inplace=True)
    
    
    
    
    date_format1 = pandas.to_datetime(BT_ow_cm_part_df['Participant contact date'], errors='coerce', format='%Y-%m-%d')
    for i in range(len(date_format1)):
        if type(date_format1[i]) != pandas._libs.tslibs.nattype.NaTType:
            date_format1[i] = date_format1[i].strftime('%Y-%m-%d %H:%M:%S:%f')
                #print(i)
    date_format2 = pandas.to_datetime(BT_ow_cm_part_df['Participant contact date'], errors='coerce', format='%Y-%m-%d %H:%M:%S')
    for i in range(len(date_format2)):
        if type(date_format2[i]) != pandas._libs.tslibs.nattype.NaTType:
            date_format2[i] = date_format2[i].strftime('%Y-%m-%d %H:%M:%S:%f')
    date_format3 = pandas.to_datetime(BT_ow_cm_part_df['Participant contact date'], errors='coerce', format='%m/%d/%Y')
    for i in range(len(date_format3)):
        if type(date_format3[i]) != pandas._libs.tslibs.nattype.NaTType:
            date_format3[i] = date_format3[i].strftime('%Y-%m-%d %H:%M:%S:%f')
    date_format4 = pandas.to_datetime(BT_ow_cm_part_df['Participant contact date'], errors='coerce', format='%m/%d/%Y %H:%M:%S')
    for i in range(len(date_format4)):
        if type(date_format4[i]) != pandas._libs.tslibs.nattype.NaTType:
            date_format4[i] = date_format4[i].strftime('%Y-%m-%d %H:%M:%S:%f')
            
    date_format1 = date_format1.fillna(date_format2)
    date_format1 = date_format1.fillna(date_format3)
    BT_ow_cm_part_df['Participant contact date'] = date_format1.fillna(date_format4)  
    #will need to adjust for additional date formats as they come
    
    if start_date!=None or end_date!=None:
        disk_engine = create_engine('sqlite:///my_lite_store.db')
        BT_ow_cm_part_df.to_sql('sql_table', disk_engine, if_exists='replace',index=False)
        
        if start_date!=None and end_date!=None:
            index_str='"Participant contact date" >= '+'"'+str(start)+'"'+' AND "Participant contact date" <= '+'"'+str(end)+'"'
        elif start_date!=None:
            index_str='"Participant contact date" >= '+'"'+str(start)+'"'
        else:
            index_str='"Participant contact date" <= '+'"'+str(end)+'"'
        
        #print(index_str)
        BT_OW_CM_NOTES = pandas.read_sql_query(f'SELECT * FROM sql_table WHERE {index_str}',disk_engine)
    else:
        BT_OW_CM_NOTES = BT_ow_cm_part_df
   
    for column in BT_OW_CM_NOTES.columns:
        if 'date' in column.lower():
            date_format3 = pandas.to_datetime(BT_OW_CM_NOTES[column], errors='coerce', format='%Y-%m-%d %H:%M:%S:%f').dt.date
            BT_OW_CM_NOTES[column] = date_format3
    
    
    
    
    BT_OW_CM_NOTES=BT_OW_CM_NOTES.merge(BT_ow_cm_part_df_intake,how='left',on='Case-safe ID (18 digits)')
    
    
    BT_OW_CM_NOTES['Participant contact time']='N/A'
    
    
    #BT_OW_CM_NOTES = staff_funding(BT_OW_CM_NOTES,'Participant assigned worker',date_col="Participant contact date",org = 'ORGANIZATION_1')
     
            #check case name for OW/CM
    
    
    
    BT_OW_CM_NOTES['Form'] = None
    
    BT_OW_CM_NOTES['Form'][BT_OW_CM_NOTES['Case Management/Outreach']=='Violence Prevention'] = 'Case management participant contact'
    BT_OW_CM_NOTES['Form'][BT_OW_CM_NOTES['Case Management/Outreach']=='Violence Prevention Outreach'] = 'Outreach participant contact'
    
    BT_OW_CM_NOTES.replace({'Violence Prevention':'Case Management','Violence Prevention Outreach':'Outreach'}, inplace = True)
    #print(BT_OW_CM_NOTES.columns)
    #BT_OW_CM_NOTES = staff_search(BT_OW_CM_NOTES,'ORGANIZATION_1','Participant assigned worker',worker=False,outreach_form='Outreach participant contact',cm_form='Case management participant contact')
    #BT_OW_CM_NOTES.sort_values(by='OW or CM participant? (OW/CM)', inplace=True)
    #BT_OW_CM_NOTES.to_excel('BT_PART_TEST_1.xlsx')
    #BT_OW_CM_NOTES.drop_duplicates(subset='Case-safe ID (18 digits)', keep='first', inplace=True, ignore_index=True)
    #BT_OW_CM_NOTES.reset_index(drop=True,inplace=True)
    BT_OW_CM_NOTES['Organization']='ORGANIZATION_1'
    BT_OW_CM_NOTES.rename(columns={'Case-safe ID (18 digits)':'Participant ID'},inplace=True)
    
    case_notes.append(BT_OW_CM_NOTES)


    #BT_OW_CM_NOTES.to_excel('TEST2.xlsx')


    #ORGANIZATION_2
    
    stbc_contacts = pandas.read_excel("C:\\Users\\HansenMade\\Python Files\\ORGANIZATION_2 compiled data\\Cleaned\\StBC Casenotes.xlsx")
    stbc_contacts['Form']='Case management participant contact'
    stbc_contacts.rename(columns={'Unique ID':'Participant ID','Administered by':'Participant contact staff member','Date of Activity':'Participant contact date','All - Contact duration':'Participant contact time'},inplace=True)
    cm_contacts = pandas.read_excel("C:\\Users\\HansenMade\\Python Files\\ORGANIZATION_2 compiled data\\Cleaned\\ORGANIZATION_2_participant case notes CM.xlsx")
    cm_contacts['Form']='Case management participant contact'
    cm_contacts.rename(columns={'Unique ID':'Participant ID','Administered by':'Participant contact staff member','Date of Activity':'Participant contact date','All - Duration of contact':'Participant contact time'},inplace=True)
    ow_contacts = pandas.read_excel("C:\\Users\\HansenMade\\Python Files\\ORGANIZATION_2 compiled data\\Cleaned\\ORGANIZATION_2 participant case note OW.xlsx")
    ow_contacts['Form']='Outreach participant contact'
    ow_contacts.rename(columns={'Unique ID':'Participant ID','Administered by':'Participant contact staff member','Date of Activity':'Participant contact date','All - Duration of contact':'Participant contact time'},inplace=True)
    
    en_concatcs = pandas.concat([ow_contacts,cm_contacts,stbc_contacts], ignore_index=True)
     
    en_concatcs['Organization']='ORGANIZATION_2'
    
    
    date_format1 = pandas.to_datetime(en_concatcs['Participant contact date'], errors='coerce', format='%Y-%m-%d')
    for i in range(len(date_format1)):
        if type(date_format1[i]) != pandas._libs.tslibs.nattype.NaTType:
            date_format1[i] = date_format1[i].strftime('%Y-%m-%d %H:%M:%S:%f')
                #print(i)
    date_format2 = pandas.to_datetime(en_concatcs['Participant contact date'], errors='coerce', format='%Y-%m-%d %H:%M:%S')
    for i in range(len(date_format2)):
        if type(date_format2[i]) != pandas._libs.tslibs.nattype.NaTType:
            date_format2[i] = date_format2[i].strftime('%Y-%m-%d %H:%M:%S:%f')
    date_format3 = pandas.to_datetime(en_concatcs['Participant contact date'], errors='coerce', format='%m/%d/%Y')
    for i in range(len(date_format3)):
        if type(date_format3[i]) != pandas._libs.tslibs.nattype.NaTType:
            date_format3[i] = date_format3[i].strftime('%Y-%m-%d %H:%M:%S:%f')
    date_format4 = pandas.to_datetime(en_concatcs['Participant contact date'], errors='coerce', format='%m/%d/%Y %H:%M:%S')
    for i in range(len(date_format4)):
        if type(date_format4[i]) != pandas._libs.tslibs.nattype.NaTType:
            date_format4[i] = date_format4[i].strftime('%Y-%m-%d %H:%M:%S:%f')
            
    date_format1 = date_format1.fillna(date_format2)
    date_format1 = date_format1.fillna(date_format3)
    en_concatcs['Participant contact date'] = date_format1.fillna(date_format4)  
    #will need to adjust for additional date formats as they come
    
    if start_date!=None or end_date!=None:
        disk_engine = create_engine('sqlite:///my_lite_store.db')
        en_concatcs.to_sql('sql_table', disk_engine, if_exists='replace',index=False)
        
        if start_date!=None and end_date!=None:
            index_str='"Participant contact date" >= '+'"'+str(start)+'"'+' AND "Participant contact date" <= '+'"'+str(end)+'"'
        elif start_date!=None:
            index_str='"Participant contact date" >= '+'"'+str(start)+'"'
        else:
            index_str='"Participant contact date" <= '+'"'+str(end)+'"'
        
        #print(index_str)
        EN_OW_CM_NOTES = pandas.read_sql_query(f'SELECT * FROM sql_table WHERE {index_str}',disk_engine)
    else:
        EN_OW_CM_NOTES = en_concatcs
   
    for column in EN_OW_CM_NOTES.columns:
        if 'date' in column.lower():
            date_format3 = pandas.to_datetime(EN_OW_CM_NOTES[column], errors='coerce', format='%Y-%m-%d %H:%M:%S:%f').dt.date
            EN_OW_CM_NOTES[column] = date_format3
    
    case_notes.append(EN_OW_CM_NOTES)
    
    case_notes_df = pandas.concat(case_notes, ignore_index=True)
    
    needed_cols=['Participant ID','Form','Organization','Participant contact staff member','Participant contact date','Participant contact time']
    dropped_cols=[]
    for i in case_notes_df.columns:
        if i  not in needed_cols:
            dropped_cols.append(i)
    case_notes_df.drop(columns=dropped_cols,inplace=True)
    case_notes_df.reset_index(drop=True,inplace=True)
    
    #case_notes_df.to_excel('TEST2.xlsx')
    final_dfs.append(case_notes_df)
    
    
    
    #referrals
    """Case management participant referrals (connections) to services:
    Participant ID
    Form
    Organization
    Participant referral staff
    Participant referral date
    Participant referral type (Education, Employment, Food assistance, Health, Housing, Identification documents, Legal, Mental health, Substance use)

    """
    #BT
    bt_refrerals = pandas.read_excel("C:\\Users\\HansenMade\\Python Files\\ORGANIZATION_1 Compiled Data\cleaned\\ORGANIZATION_1_referral.xlsx")
    bt_refrerals.rename(columns={'Services Needed':'Participant referral type (Education, Employment, Food assistance, Health, Housing, Identification documents, Legal, Mental health, Substance use)','Case-Safe Contact ID':'Participant ID','Referring From Staff':'Participant referral staff','Referral Date':'Participant referral date'},inplace=True)
    bt_refrerals['Form']='Case management participant referrals (connections) to services'
    bt_refrerals['Organization']='ORGANIZATION_1'
    
    #EN_OW_CM_NOTES
    en_referrals = pandas.read_excel("C:\\Users\\HansenMade\\Python Files\\ORGANIZATION_2 compiled data\\Cleaned\\ORGANIZATION_2_referral.xlsx")
    en_referrals.rename(columns={'Unique ID':'Participant ID','Administered by':'Participant referral staff','Date of Activity':'Participant referral date','All - Type of referral':'Participant referral type (Education, Employment, Food assistance, Health, Housing, Identification documents, Legal, Mental health, Substance use)'},inplace=True)
    stbs_referals = pandas.read_excel("C:\\Users\\HansenMade\\Python Files\\ORGANIZATION_2 compiled data\\Cleaned\\StBC Referrals.xlsx")
    stbs_referals.rename(columns={'Case Manager':'Participant referral staff','Linked Date':'Participant referral date','Linkage Type':'Participant referral type (Education, Employment, Food assistance, Health, Housing, Identification documents, Legal, Mental health, Substance use)'},inplace=True)
    en_referrals_all = pandas.concat([en_referrals,stbs_referals], ignore_index=True)
    en_referrals_all['Form']='Case management participant referrals (connections) to services'
    en_referrals_all['Organization']='ORGANIZATION_2'
    
    all_refs = pandas.concat([bt_refrerals,en_referrals_all], ignore_index=True)
    
    
    
    date_format1 = pandas.to_datetime(all_refs['Participant referral date'], errors='coerce', format='%Y-%m-%d')
    for i in range(len(date_format1)):
        if type(date_format1[i]) != pandas._libs.tslibs.nattype.NaTType:
            date_format1[i] = date_format1[i].strftime('%Y-%m-%d %H:%M:%S:%f')
                #print(i)
    date_format2 = pandas.to_datetime(all_refs['Participant referral date'], errors='coerce', format='%Y-%m-%d %H:%M:%S')
    for i in range(len(date_format2)):
        if type(date_format2[i]) != pandas._libs.tslibs.nattype.NaTType:
            date_format2[i] = date_format2[i].strftime('%Y-%m-%d %H:%M:%S:%f')
    date_format3 = pandas.to_datetime(all_refs['Participant referral date'], errors='coerce', format='%m/%d/%Y')
    for i in range(len(date_format3)):
        if type(date_format3[i]) != pandas._libs.tslibs.nattype.NaTType:
            date_format3[i] = date_format3[i].strftime('%Y-%m-%d %H:%M:%S:%f')
    date_format4 = pandas.to_datetime(all_refs['Participant referral date'], errors='coerce', format='%m/%d/%Y %H:%M:%S')
    for i in range(len(date_format4)):
        if type(date_format4[i]) != pandas._libs.tslibs.nattype.NaTType:
            date_format4[i] = date_format4[i].strftime('%Y-%m-%d %H:%M:%S:%f')
            
    date_format1 = date_format1.fillna(date_format2)
    date_format1 = date_format1.fillna(date_format3)
    all_refs['Participant referral date'] = date_format1.fillna(date_format4)  
    #will need to adjust for additional date formats as they come
    
    if start_date!=None or end_date!=None:
        disk_engine = create_engine('sqlite:///my_lite_store.db')
        all_refs.to_sql('sql_table', disk_engine, if_exists='replace',index=False)
        
        if start_date!=None and end_date!=None:
            index_str='"Participant referral date" >= '+'"'+str(start)+'"'+' AND "Participant referral date" <= '+'"'+str(end)+'"'
        elif start_date!=None:
            index_str='"Participant referral date" >= '+'"'+str(start)+'"'
        else:
            index_str='"Participant referral date" <= '+'"'+str(end)+'"'
        
        #print(index_str)
        all_refs_FINAL = pandas.read_sql_query(f'SELECT * FROM sql_table WHERE {index_str}',disk_engine)
    else:
        all_refs_FINAL = all_refs
   
    for column in all_refs_FINAL.columns:
        if 'date' in column.lower():
            date_format3 = pandas.to_datetime(all_refs_FINAL[column], errors='coerce', format='%Y-%m-%d %H:%M:%S:%f').dt.date
            all_refs_FINAL[column] = date_format3
    
    
    needed_cols=['Participant ID','Form','Organization','Participant referral staff','Participant referral date','Participant referral type (Education, Employment, Food assistance, Health, Housing, Identification documents, Legal, Mental health, Substance use)']
    dropped_cols=[]
    for i in all_refs_FINAL.columns:
        if i  not in needed_cols:
            dropped_cols.append(i)
    all_refs_FINAL.drop(columns=dropped_cols,inplace=True)
    all_refs_FINAL.reset_index(drop=True,inplace=True)
    
    #case_notes_df.to_excel('TEST2.xlsx')
    final_dfs.append(all_refs_FINAL)
    
    #incident responses
    
    """" Incident responses:
    Form
    Organization
    Staff member(s) that contributed to the incident response
    Incident notification date/time
    Incident response date/time
    CPIC address
    Incident address"""
    
    
    #BT
    
    bt_incidents = pandas.read_excel("C:\\Users\\HansenMade\\Python Files\\ORGANIZATION_1 Compiled Data\\cleaned\ORGANIZATION_1_incident_by_incident.xlsx")
    bt_incidents.rename(columns = {'Staff entering report':'Staff member(s) that contributed to the incident response','Date Notified':'Incident notification date/time','Address/Cross Streets':'Incident address'},inplace=True)
    bt_incidents['Incident response date/time']='N/A'
    bt_incidents['Incident notification date/time']=bt_incidents['Incident notification date/time'].astype(str)
    bt_incidents['Form']='Incident response'
    bt_incidents['Organization']='ORGANIZATION_1'
    bt_incidents['CPIC address']=None
    bt_incidents['Incident Date'] = bt_incidents['Incident notification date/time'].copy()
    
    needed_cols=['Incident Date','Form','Organization','Staff member(s) that contributed to the incident response','Incident notification date/time','Incident response date/time','CPIC address','Incident address']
    dropped_cols=[]
    for i in bt_incidents.columns:
        if i  not in needed_cols:
            dropped_cols.append(i)
    bt_incidents.drop(columns=dropped_cols,inplace=True)
    bt_incidents.reset_index(drop=True,inplace=True)
    
    #EN
    en_incidents = pandas.read_excel("C:\\Users\\HansenMade\\Python Files\\ORGANIZATION_2 compiled data\\Cleaned\\ORGANIZATION_2 Incidents.xlsx")
    en_incidents.rename(columns = {'Street outreach staff responding to incident':'Staff member(s) that contributed to the incident response','Address/Cross streets':'Incident address'},inplace=True)
    en_incidents['Incident response date/time'] = en_incidents['Date of response'].astype(str) +' '+en_incidents['Time of response'].astype(str)
    en_incidents['Incident notification date/time'] = en_incidents['Date of notification'].astype(str) +' '+en_incidents['Time of notification'].astype(str)

    en_incidents['Form']='Incident response'
    en_incidents['Organization']='ORGANIZATION_2'
    en_incidents['CPIC address']=None
    en_incidents['Incident Date'] = en_incidents['Date of Violent Incident'].copy()
    
    
    all_in = pandas.concat([bt_incidents,en_incidents], ignore_index=True)
    
    
    
    date_format1 = pandas.to_datetime(all_in['Incident Date'], errors='coerce', format='%Y-%m-%d')
    for i in range(len(date_format1)):
        if type(date_format1[i]) != pandas._libs.tslibs.nattype.NaTType:
            date_format1[i] = date_format1[i].strftime('%Y-%m-%d %H:%M:%S:%f')
                #print(i)
    date_format2 = pandas.to_datetime(all_in['Incident Date'], errors='coerce', format='%Y-%m-%d %H:%M:%S')
    for i in range(len(date_format2)):
        if type(date_format2[i]) != pandas._libs.tslibs.nattype.NaTType:
            date_format2[i] = date_format2[i].strftime('%Y-%m-%d %H:%M:%S:%f')
    date_format3 = pandas.to_datetime(all_in['Incident Date'], errors='coerce', format='%m/%d/%Y')
    for i in range(len(date_format3)):
        if type(date_format3[i]) != pandas._libs.tslibs.nattype.NaTType:
            date_format3[i] = date_format3[i].strftime('%Y-%m-%d %H:%M:%S:%f')
    date_format4 = pandas.to_datetime(all_in['Incident Date'], errors='coerce', format='%m/%d/%Y %H:%M:%S')
    for i in range(len(date_format4)):
        if type(date_format4[i]) != pandas._libs.tslibs.nattype.NaTType:
            date_format4[i] = date_format4[i].strftime('%Y-%m-%d %H:%M:%S:%f')
            
    date_format1 = date_format1.fillna(date_format2)
    date_format1 = date_format1.fillna(date_format3)
    all_in['Incident Date'] = date_format1.fillna(date_format4)  
    #will need to adjust for additional date formats as they come
    
    if start_date!=None or end_date!=None:
        disk_engine = create_engine('sqlite:///my_lite_store.db')
        all_in.to_sql('sql_table', disk_engine, if_exists='replace',index=False)
        
        if start_date!=None and end_date!=None:
            index_str='"Incident Date" >= '+'"'+str(start)+'"'+' AND "Incident Date" <= '+'"'+str(end)+'"'
        elif start_date!=None:
            index_str='"Incident Date" >= '+'"'+str(start)+'"'
        else:
            index_str='"Incident Date" <= '+'"'+str(end)+'"'
        
        #print(index_str)
        all_in_FINAL = pandas.read_sql_query(f'SELECT * FROM sql_table WHERE {index_str}',disk_engine)
    else:
        all_in_FINAL = all_in
   
    
    
    
    needed_cols=['Form','Organization','Staff member(s) that contributed to the incident response','Incident notification date/time','Incident response date/time','CPIC address','Incident address']
    dropped_cols=[]
    for i in all_in_FINAL.columns:
        if i  not in needed_cols:
            dropped_cols.append(i)
    all_in_FINAL.drop(columns=dropped_cols,inplace=True)
    all_in_FINAL.reset_index(drop=True,inplace=True)
    
    #case_notes_df.to_excel('TEST2.xlsx')
    final_dfs.append(all_in_FINAL)
    

    final_df = pandas.concat(final_dfs, ignore_index=True)
    final_df.to_excel('scorecard.xlsx')
    


rc_nonrc_merge(start_date='2024-7-1',end_date='2024-9-30')