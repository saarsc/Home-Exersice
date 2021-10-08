from requests import get
import pandas as pd
import csv
import boto3
import mysql.connector
import sys
from contextlib import contextmanager
from sqlite3 import connect
from multiprocessing import Pool
from botocore.exceptions import ClientError

DATA_FILE = "data.csv"
RESULT_FILE = "result.csv"

@contextmanager
def getCon():     
    try:
        conn = mysql.connector.connect(
            user="root",
            password="example",
            host="localhost",
            port=3306,
        )

        yield conn
        conn.close()
    except Exception as e:
            print(f"Error connecting to MySql Platform: {e}")
            sys.exit(1)
@contextmanager
def getCursor(conn,init= False):
    cur = conn.cursor()
    cur.execute("SET GLOBAL sql_mode = ''")
    if(init):
        cur.execute("CREATE DATABASE IF NOT EXISTS data")
    cur.execute("USE data")
    yield cur

    cur.close()
def insert(rows):
    with getCon() as conn:
        with getCursor(conn) as cur:
            qMarks = "%s".join(len(rows[0]) * ",")[1:] + "%s"
            cur.executemany(f"INSERT INTO data VALUES({qMarks})",rows)
            conn.commit()

def withOutPandas()-> None:
        print("Parsing without Pandas...")
        # DB initialization
        with getCon() as conn:
            with getCursor(conn,True) as cur:
                cur.execute("CREATE TABLE IF NOT EXISTS data (iso_code TEXT,continent TEXT,location TEXT,date DATE,total_cases FLOAT,new_cases FLOAT,new_cases_smoothed FLOAT,total_deaths FLOAT,new_deaths FLOAT,new_deaths_smoothed FLOAT,total_cases_per_million FLOAT,new_cases_per_million FLOAT,new_cases_smoothed_per_million FLOAT,total_deaths_per_million FLOAT,new_deaths_per_million FLOAT,new_deaths_smoothed_per_million FLOAT,reproduction_rate FLOAT,icu_patients FLOAT,icu_patients_per_million FLOAT,hosp_patients FLOAT,hosp_patients_per_million FLOAT,weekly_icu_admissions FLOAT,weekly_icu_admissions_per_million FLOAT,weekly_hosp_admissions FLOAT,weekly_hosp_admissions_per_million FLOAT,new_tests FLOAT,total_tests FLOAT,total_tests_per_thousand FLOAT,new_tests_per_thousand FLOAT,new_tests_smoothed FLOAT,new_tests_smoothed_per_thousand FLOAT,positive_rate FLOAT,tests_per_case FLOAT,tests_units TEXT,total_vaccinations FLOAT,people_vaccinated FLOAT,people_fully_vaccinated FLOAT,total_boosters FLOAT,new_vaccinations FLOAT,new_vaccinations_smoothed FLOAT,total_vaccinations_per_hundred FLOAT,people_vaccinated_per_hundred FLOAT,people_fully_vaccinated_per_hundred FLOAT,total_boosters_per_hundred FLOAT,new_vaccinations_smoothed_per_million FLOAT,stringency_index FLOAT,population FLOAT,population_density FLOAT,median_age FLOAT,aged_65_older FLOAT,aged_70_older FLOAT,gdp_per_capita FLOAT,extreme_poverty FLOAT,cardiovasc_death_rate FLOAT,diabetes_prevalence FLOAT,female_smokers FLOAT,male_smokers FLOAT,handwashing_facilities FLOAT,hospital_beds_per_thousand FLOAT,life_expectancy FLOAT,human_development_index FLOAT,excess_mortality_cumulative_absolute FLOAT,excess_mortality_cumulative FLOAT,excess_mortality FLOAT,excess_mortality_cumulative_per_million FLOAT)")
                cur.execute("TRUNCATE data")

        with open(DATA_FILE) as f:
            data = list(csv.reader(f))
            print("Inserting data to DB...")

            SLICE_SIZE = len(data) // 100
            rows = [data[i:i + SLICE_SIZE] for i in range(1, len(data), SLICE_SIZE)]
            with Pool(2) as p:
                p.map(insert,rows)
            
            print("Finished inserting data...")
            with getCon() as conn:
                        with getCursor(conn) as cur:
                            cur.execute("SELECT DISTINCT location FROM data")
                            result =cur.fetchall()
                            with open(RESULT_FILE,"w", newline='') as r:
                                writer = csv.DictWriter(r,fieldnames=["Uniqe Countries"])
                                writer.writeheader()
                                writer.writerow({"Uniqe Countries":len(result)})
        
if __name__ =="__main__":
    """
    Upload the file results.csv into the demo bucket under the name result.csv 
    """
    def pushToS3()-> None:
        print("Uploading to s3...")
        s3 = boto3.client("s3",endpoint_url="http://localhost:4566")
        if(not s3.head_bucket(Bucket="demo")):
            s3.create_bucket(Bucket='demo')
        try:
            s3.upload_file("result.csv","demo","result.csv")
            print("Finished uploading to s3...")
        except ClientError as e:
            print("Error uploading file to S3")
    """
        Downloads the desierd CSV file and saves it as data.csv
    """
    def downloadFile()-> None:
            with open(DATA_FILE,"wb") as f:
                f.write(get("https://covid.ourworldindata.org/data/owid-covid-data.csv").text.encode())
    """
        Super quick method. Uses Pandas to parse the data and saves it to local sql file
    """
    def withPandas()-> None:
        """
            Parses the data.csv file, creates the local DB file and the result file
        """
        def parseFile()-> None:
            print("Parsing file with Pandas...")
            with open(DATA_FILE) as f:
                data = pd.read_csv(f)
                db = connect("result.db")

                data.to_sql("data",db,if_exists="replace")

                result = pd.DataFrame({"Uniqe Countries":[len(set(data["location"]))]})

                with open(RESULT_FILE,"w") as f:
                    f.write(result.to_csv(index=False))
            print("Finsied parsing...")
        parseFile()
    """
        Longer method, manual parsing, and hosted DB.
    """
    
            
    # downloadFile()

    # withPandas()
    withOutPandas()

    pushToS3()
