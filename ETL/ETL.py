#pip install pandas

import glob
import pandas as pd
import xml.etree.ElementTree as ET
from datetime import datetime

log_file = "log_file.txt"
target_file = "transformed_data.csv"

# Functions to extract data from csv, json & xml files
def extract_from_csv(file_to_process):
    df = pd.read_csv(file_to_process)
    return df

def extract_from_json(file_to_process):
    df = pd.read_json(file_to_process, lines=True)
    return df

def extract_from_xml(file_to_process):
    df = pd.DataFrame(columns=["car_model","year_of_manufacture","price","fuel"])
    tree = ET.parse(file_to_process)
    root = tree.getroot()
    for car in root.findall("car"):
        car_model = car.find("car_model").text
        year_of_manufacture = int(car.find("year_of_manufacture").text)
        price = float(car.find("price").text)
        fuel = car.find("fuel").text
        df = pd.concat([df, pd.DataFrame([{"car_model":car_model, "year_of_manufacture":year_of_manufacture, "price":price, "fuel":fuel}])], ignore_index=True)
        return df
    
def extract():
    extracted_data = pd.DataFrame(columns=["car_model","year_of_manufacture","price","fuel"])
    
    for csvfile in glob.glob("*.csv"):
        if csvfile != target_file:
            extracted_data = pd.concat([extracted_data, pd.DataFrame(extract_from_csv(csvfile))], ignore_index=True)

    for jsonfile in glob.glob("*.json"):
        extracted_data = pd.concat([extracted_data, pd.DataFrame(extract_from_json(jsonfile))], ignore_index=True)

    for xmlfile in glob.glob("*.xml"):
        extracted_data = pd.concat([extracted_data, pd.DataFrame(extract_from_xml(xmlfile))], ignore_index=True)

    return extracted_data

# Rounding 'price' to 2 decimal places
def transform(data):
    data['price'] = round(data.price,2)

    return data

# Transferring transformed data to new file
def load_data(target_file, transformed_data):
    transformed_data.to_csv(target_file)

# Output record of processes with timestamps into a log file
def log_progress(message):
    timestamp_format = '%Y-%h-%d-%H:%M:%S'
    now = datetime.now()
    timestamp = now.strftime(timestamp_format)
    with open(log_file, "a") as f:
        f.write(timestamp + ',' + message + '\n')

log_progress("ETL Job Started")

log_progress("Extract Phase Started")

extracted_data = extract()

log_progress("Extract Phase Ended")

log_progress("Transform Phase Started")
transformed_data = transform(extracted_data)
print("Transformed Data")
print(transformed_data)

log_progress("Transform Phase Ended")

log_progress("Load Phase Started")
load_data(target_file, transformed_data)

log_progress("Load Phase Ended")

log_progress("ETL Job Ended")
