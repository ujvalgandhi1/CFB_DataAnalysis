#Season Stats by Year
import requests, json
import pandas as pd
from datetime import datetime 

# Define a list of years from 2004 (when the api has data) through current year
current_year = datetime.now().year + 1 
starting_year = datetime.now().year - 19


years = list(range(starting_year, current_year))

# Initialize an empty list to store DataFrames
dfs = []

# Loop through each year
for year in years:    
    url = f"https://api.collegefootballdata.com/stats/season?year={year}"
    payload = {}
    headers = {
        'accept': 'application/json',
        'Authorization': 'Bearer <Enter your API Key here>'
    }
    response = requests.request("GET", url, headers=headers, data=payload)
    # Check if the request was successful
    if response.status_code == 200:
        # Parse the JSON response into a Python dictionary
        data = json.loads(response.text)
        df = pd.DataFrame(data)
        # Append the DataFrame to the list
        dfs.append(df)

    else:
        print(f"API request failed for year {year} with status code:", response.status_code)

# Concatenate all DataFrames into one
final_df = pd.concat(dfs, ignore_index=True)
#Writing data to the lakehouse
sparkdf = spark.createDataFrame(final_df)
sparkdf.write.mode("overwrite").format("csv").save("Files/CFB/SeasonStats")
table_name = 'CFB_SeasonStats'
sparkdf.write.mode("overwrite").format("delta").save("Tables/" + table_name)