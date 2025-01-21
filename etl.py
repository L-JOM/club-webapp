from curses.ascii import isdigit
import json
import requests
import pandas as pd
from datetime import datetime


#extract json file and 
def extract_json(url):
    try:
        
        response = requests.get(url)
        response.raise_for_status()
        json_file = response.json()
        
    except requests.RequestException as e:
        raise Exception(f"Error fetching URL {url}: {e}")
    except json.JSONDecodeError as e:
        raise ValueError(f"Invalid JSON format in {json_file}: {e}")  
    return json_file


#transform json file
def transform_json_to_list(json):
    course_list = []
    caches = json['caches']
    courses = json['courses']
    
    for i in courses:
        
        sections = courses[i][1]
        
        for l in sections:
            course_format = {}
            course_details = sections[l]
            course_format['crn'] = course_details[0]
            course_format['name'] = i
            course_format['actual_name'] = courses[i][0]
            course_format['section'] = l
            
            if course_details[1]:
                section_details = course_details[1][0]
                course_format['time'] = caches['periods'][section_details[0]]
                course_format['day_of_week'] = section_details[1]
                course_format['building'] = section_details[2]
                course_format['building_coords'] = caches['locations'][section_details[3]]
                course_format['professors'] = section_details[4]
                course_format['date_range'] = caches['dateRanges'][section_details[5]]
            else:
                section_details = None
            course_format['credits'] = course_details[2]
            course_format['schedule_type'] = caches['scheduleTypes'][course_details[3]]
            course_format['campus'] = caches['campuses'][course_details[4]]
            
            course_format['attributes'] = course_details[5]
            for a in course_format['attributes']:
                course_format['attributes'] = caches['attributes'][a]
            course_list.append(course_format)
    return course_list

#transform dataframe to needs
def transform(course_list):
    df = pd.DataFrame(course_list)
    
    df.drop(['attributes','building_coords','professors'],axis=1, inplace=True)
    
    df[['start_date','end_date']] = df['date_range'].str.split(' - ', expand=True)
    df[['start_time','end_time']] = df['time'].str.split(' - ', expand=True)
    
    
    df.drop(['date_range', 'time'], axis=1, inplace=True)
    
    #The model is going to require a start-endtime for classes so we have to drop any that doesn't have it    
    df.drop(df[df[['start_time', 'end_time']].isna().any(axis=1)].index,inplace=True)
    
    def transform_military_time(military_time):
        if military_time.isdigit():
            dt = datetime.strptime(military_time, '%H%M')
            return dt.time()
    
    df['start_time'] = df['start_time'].apply(transform_military_time)
    df['end_time'] = df['end_time'].apply(transform_military_time)
    
    df['monday'] = df['day_of_week'].str.contains('M', na=False)
    df['tuesday'] = df['day_of_week'].str.contains('T', na=False)
    df['wednesday'] = df['day_of_week'].str.contains('W', na=False)
    df['thurday'] = df['day_of_week'].str.contains('R', na=False)
    df['friday'] = df['day_of_week'].str.contains('F', na=False)
    df.drop(['day_of_week'], axis=1, inplace=True)
    
    df['building'] = df['building'].str.extract(r'(.*) \w+\d')
    
    return df

#transform dataframe to sql

def df_to_sql(df):
    ddl = pd.io.sql.get_schema(df, 'data')
    return ddl
       
if __name__  == "__main__":
    url = 'https://gt-scheduler.github.io/crawler-v2/202502.json'
    json_courses = extract_json(url)
    course_list = transform_json_to_list(json_courses)
    df = transform(course_list)
    print()
    