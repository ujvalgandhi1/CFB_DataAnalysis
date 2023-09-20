#Win-Loss Records by Year
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
    url = f"https://api.collegefootballdata.com/records?year={year}"

    payload = {}
    headers = {
        'accept': 'application/json',
        'Authorization': 'Bearer <enter your API Key here>'
    }

    response = requests.request("GET", url, headers=headers, data=payload)

    # Check if the request was successful
    if response.status_code == 200:
        # Parse the JSON response into a Python dictionary
        data = json.loads(response.text)
        df = pd.DataFrame(data)
        total_df = pd.DataFrame(df['total'].tolist())

        total_df = total_df.rename(columns={
            'games': 'totalgames',
            'wins': 'totalwins',
            'losses': 'totallosses',
            'ties': 'totalties'
        })

        df = pd.concat([df, total_df], axis=1)

        conferencegames_df = pd.DataFrame(df['conferenceGames'].tolist())
        conferencegames_df = conferencegames_df.rename(columns={
            'games': 'conferencegames',
            'wins': 'conferencewins',
            'losses': 'conferencelosses',
            'ties': 'conferenceties'
        })

        df = pd.concat([df, conferencegames_df], axis=1)

        homegames_df = pd.DataFrame(df['homeGames'].tolist())
        homegames_df = homegames_df.rename(columns={
            'games': 'homegames',
            'wins': 'homewins',
            'losses': 'homelosses',
            'ties': 'hometies'
        })

        df = pd.concat([df, homegames_df], axis=1)

        awaygames_df = pd.DataFrame(df['awayGames'].tolist())
        awaygames_df = awaygames_df.rename(columns={
            'games': 'awaygames',
            'wins': 'awaywins',
            'losses': 'awaylosses',
            'ties': 'awayties'
        })

        df = pd.concat([df, awaygames_df], axis=1)

        df = df.drop(columns=['total'])
        df = df.drop(columns=['conferenceGames'])
        df = df.drop(columns=['homeGames'])
        df = df.drop(columns=['awayGames'])

        # Append the DataFrame to the list
        dfs.append(df)

    else:
        print(f"API request failed for year {year} with status code:", response.status_code)

# Concatenate all DataFrames into one
final_df = pd.concat(dfs, ignore_index=True)

#Writing data to the lakehouse
sparkdf = spark.createDataFrame(final_df)
sparkdf.write.mode("overwrite").format("csv").save("Files/CFB/TeamWinLossRecords")

table_name = 'CFB_WinLoss'
sparkdf.write.mode("overwrite").format("delta").save("Tables/" + table_name)
