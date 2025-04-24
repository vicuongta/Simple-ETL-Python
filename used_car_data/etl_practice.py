import pandas as pd
import glob
from datetime import datetime
import xml.etree.ElementTree as ET

log_file = "log_file.txt"
target_file = "transformed_data.csv"

# Task 1: Extraction
def extract_from_csv(filename):
    df = pd.read_csv(filename)
    return df

def extract_from_json(filename):
    df = pd.read_json(filename, lines=True)
    return df

def extract_from_xml(filename):
    df = pd.DataFrame(columns=['car_model', 'year_of_manufacture', 'price', 'fuel'])
    tree = ET.parse(filename)
    root = tree.getroot()
    for child in root:
        car_model = child.find('car_model').text
        year_of_manufacture = int(child.find('year_of_manufacture').text)
        price = float(child.find('price').text)
        fuel = child.find('fuel').text
        df = pd.concat([df, pd.DataFrame([
            {
                'car_model': car_model,
                "year_of_manufacture": year_of_manufacture,
                "price": price,
                "fuel": fuel
            }])], ignore_index=True)

    return df

def extract():
    extracted_data = pd.DataFrame(
        columns=['car_model', 'year_of_manufacture', 'price', 'fuel'] # initialize an empty dataframe
    )

    # process all csv files, except the target file
    for csvfile in glob.glob("*.csv"):
        if csvfile != target_file:  # check if the file is not the target file
            extracted_data = pd.concat([extracted_data, pd.DataFrame(extract_from_csv(csvfile))], ignore_index=True)

            # process all json files
    for jsonfile in glob.glob("*.json"):
        extracted_data = pd.concat([extracted_data, pd.DataFrame(extract_from_json(jsonfile))], ignore_index=True)

        # process all xml files
    for xmlfile in glob.glob("*.xml"):
        extracted_data = pd.concat([extracted_data, pd.DataFrame(extract_from_xml(xmlfile))], ignore_index=True)

    return extracted_data

# -----------------------
# Task 2: Transformation
# The output of this function will now be a dataframe where the "height" and "weight" parameters will be modified to the required format.
def transform(data):
    """ Transform values under 'price' header such that they are rounded to 2 decimal places """
    data['price'] = round(data['price'], 2)

    return data

# -----------------------
# Task 3: Loading and Logging
def load_data(target_file, transformed_data):
    transformed_data.to_csv(target_file)

# implement the logging operation to record the progress of the different operations
def log_progress(message):
    timestamp_format = '%Y-%h-%d-%H:%M:%S' # Year-Monthname-Day-Hour-Minute-Second
    now = datetime.now() # get current timestamp
    timestamp = now.strftime(timestamp_format)
    with open(log_file,"a") as f:
        f.write(timestamp + ',' + message + '\n')

# -----------------------
# Testing ETL operations and log progress
# Log the initialization of the ETL process
log_progress("ETL Job Started")

# Log the beginning of the Extraction process
log_progress("Extract phase Started")
extracted_data = extract()

# Log the completion of the Extraction process
log_progress("Extract phase Ended")

# Log the beginning of the Transformation process
log_progress("Transform phase Started")
transformed_data = transform(extracted_data)
print("Transformed Data")
print(transformed_data)

# Log the completion of the Transformation process
log_progress("Transform phase Ended")

# Log the beginning of the Loading process
log_progress("Load phase Started")
load_data(target_file, transformed_data)

# Log the completion of the Loading process
log_progress("Load phase Ended")

# Log the completion of the ETL process
log_progress("ETL Job Ended")