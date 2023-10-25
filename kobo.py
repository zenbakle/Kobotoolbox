import pandas as pd
import numpy as np
import requests as rq
from requests.auth import HTTPBasicAuth
from io import StringIO
from datetime import datetime
from flask_pymongo import PyMongo

app = Flask(__name__)
app.secret_key = "secretkey"
app.config['MONGO_URI'] ="mongodb://"
mongo = PyMongo(app)

def download_data(form):
    # Define the URL with a placeholder for 'hmo'
    url = "https://kc.humanitarianresponse.info/api/v1/data/{}".format(form)

    try:
        # Attempt to make an HTTP GET request with authentication
        data = rq.get(url, auth=HTTPBasicAuth("username", "password"))

        # Check if the request was successful (HTTP status code 200)
        if data.status_code == 200:
            # Read the JSON data into a Pandas DataFrame
            df = pd.read_json(StringIO(data.text))

            # Convert the '_submission_time' column to datetime
            df['_submission_time'] = pd.to_datetime(df['_submission_time'], format="%Y-%m-%d %H:%M:%S")

            # Filter the DataFrame to include only data within a specific date range
            df = df[(df["_submission_time"] >= datetime.strptime("2022-01-01 00:00:00", "%Y-%m-%d %H:%M:%S"))
                    & (df["_submission_time"] <= datetime.strptime("2023-04-01 00:00:00", "%Y-%m-%d %H:%M:%S"))]

            # Save the filtered DataFrame to a CSV file
            df.to_csv("kobodata/data.csv", index=False)

            # Return the filtered DataFrame
            return df
        else:
            # Handle HTTP request errors
            print(f"HTTP request failed with status code {data.status_code}")
            return None

    except Exception as e:
        # Handle exceptions, such as network errors or data parsing errors
        print(f"An error occurred: {str(e)}")
        return None


def clean_drop_col(df):
    # List of columns to be dropped
    cols_to_drop = ["__version__", "_submitted_by", "formhub/uuid", "_attachments", "meta/instanceID", "_tags",
                    "_notes", "_xform_id_string", "_geolocation", "_status", "_validation_status"]

    try:
        # Check if the DataFrame is not empty
        if not df.empty:
            # Drop the specified columns from the DataFrame
            df.drop(cols_to_drop, axis=1, inplace=True)

            # Rename the columns by replacing underscores with spaces and converting to lowercase
            new_columns = [col.replace("_", " ").strip().lower().replace(" ", "_") for col in df.columns]
            df.columns = new_columns

            # Print the new column names and the shape of the DataFrame
            print("New column names:", new_columns)
            print("DataFrame shape:", df.shape)

            return df
        else:
            # Handle the case where the input DataFrame is empty
            print("Input DataFrame is empty.")
            return None

    except Exception as e:
        # Handle exceptions, such as DataFrame manipulation errors
        print(f"An error occurred: {str(e)}")
        return None



def data_to_db(df):
    try:
        # Convert the DataFrame to a list of dictionaries (records)
        data = df.to_dict("records")

        collection = mongo.db.health

        # Insert the data into the MongoDB collection
        collection.insert_many(data)

        # Close the MongoDB client connection
        client.close()

        return "Information updated"

    except Exception as e:
        # Handle exceptions, such as MongoDB connection errors or data insertion errors
        print(f"An error occurred: {str(e)}")
        return "Error: Information not updated"




if __name__ == "__main__":
    data = download_data("form")
    data = clean_drop_col(data)
    print(data_to_db(data))
