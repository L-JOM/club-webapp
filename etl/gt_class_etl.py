import json
import requests
import pandas as pd
from datetime import datetime
from sqlalchemy import create_engine, text
import psycopg2

# Extract JSON file from a given URL
def extract_json(url):
    """
    Fetches JSON data from the specified URL.

    Args:
        url (str): The URL to fetch the JSON data from.

    Returns:
        dict: Parsed JSON data.

    Raises:
        Exception: If the request to the URL fails or the response is not valid JSON.
    """
    try:
        response = requests.get(url)
        response.raise_for_status()
        json_file = response.json()
    except requests.RequestException as e:
        raise Exception(f"Error fetching URL {url}: {e}")
    except json.JSONDecodeError as e:
        raise ValueError(f"Invalid JSON format in {url}: {e}")
    return json_file

# Transform JSON data into a list of courses
def transform_json_to_list(json):
    """
    Converts the raw JSON data into a structured list of course dictionaries.

    Args:
        json (dict): The raw JSON data.

    Returns:
        list: A list of dictionaries containing structured course data.
    """
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

            course_format['credits'] = course_details[2]
            course_format['schedule_type'] = caches['scheduleTypes'][course_details[3]]
            course_format['campus'] = caches['campuses'][course_details[4]]

            # Convert attributes to their descriptive names
            course_format['attributes'] = [caches['attributes'][a] for a in course_details[5]] if course_details[5] else None
            course_list.append(course_format)
    return course_list

# Transform the list of courses into a cleaned DataFrame
def transform(course_list):
    """
    Cleans and structures the course data into a pandas DataFrame.

    Args:
        course_list (list): The list of course dictionaries.

    Returns:
        pd.DataFrame: Cleaned and structured DataFrame.
    """
    df = pd.DataFrame(course_list)

    # Drop unnecessary columns
    df.drop(['attributes', 'building_coords', 'professors'], axis=1, inplace=True)

    # Split date_range into start_date and end_date
    df[['start_date', 'end_date']] = df['date_range'].str.split(' - ', expand=True)
    
    # Split time into start_time and end_time
    df[['start_time', 'end_time']] = df['time'].str.split(' - ', expand=True)
    
    # Drop original date_range and time columns
    df.drop(['date_range', 'time'], axis=1, inplace=True)

    # Drop rows where either start_time or end_time is missing
    df.dropna(subset=['start_time', 'end_time'], inplace=True)

    # Convert military time to proper datetime.time objects
    def transform_military_time(military_time):
        if military_time.isdigit():
            return datetime.strptime(military_time, '%H%M').time()

    df['start_time'] = df['start_time'].apply(transform_military_time)
    df['end_time'] = df['end_time'].apply(transform_military_time)

    # Map days of the week to boolean columns
    df['monday'] = df['day_of_week'].str.contains('M', na=False)
    df['tuesday'] = df['day_of_week'].str.contains('T', na=False)
    df['wednesday'] = df['day_of_week'].str.contains('W', na=False)
    df['thursday'] = df['day_of_week'].str.contains('R', na=False)
    df['friday'] = df['day_of_week'].str.contains('F', na=False)
    
    # Drop the original day_of_week column
    df.drop(['day_of_week'], axis=1, inplace=True)

    # Extract building name from full building information
    df['building'] = df['building'].str.extract(r'(.*) \w+\d')

    return df

# Load the cleaned DataFrame into PostgreSQL
def df_to_sql(df):
    """
    Uploads the DataFrame into a PostgreSQL database table.

    Args:
        df (pd.DataFrame): The cleaned DataFrame to upload.

    Raises:
        Exception: If there is an issue uploading data to the database.
    """
    engine = create_engine('postgresql+psycopg2://root:password@localhost:5432/stu_org_webapp')
    with engine.connect() as conn:
        try:
            # Upload DataFrame to SQL table, replacing any existing table
            df.to_sql(name='gt_classes', con=engine, if_exists='replace', index=False)

            # Verify the table exists in the database
            query = "SELECT table_name FROM information_schema.tables WHERE table_name = 'gt_classes'"
            check = conn.execute(text(query)).fetchone()

            if check:
                print(f"'{check[0]}' table has been successfully uploaded to PostgreSQL.")
        except Exception as e:
            raise Exception(f"Error uploading data to SQL: {e}")

# Orchestrate the ETL process
def gt_class_etl():
    """
    Executes the complete ETL pipeline: Extract, Transform, and Load data.
    """
    try:
        url = 'https://gt-scheduler.github.io/crawler-v2/202502.json'
        
        # Extract data
        json_courses = extract_json(url)
        
        # Transform extracted data
        course_list = transform_json_to_list(json_courses)
        print(f"Transformed {len(course_list)} courses.")

        # Further transform the data into a structured DataFrame
        df = transform(course_list)
        print(f"DataFrame transformed: {df.shape[0]} rows, {df.shape[1]} columns.")

        # Load the DataFrame into the database
        df_to_sql(df)
    except Exception as e:
        print(f"ETL failed: {e}")

# Execute the ETL pipeline
gt_class_etl()