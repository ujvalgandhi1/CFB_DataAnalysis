#!/usr/bin/env python
# coding: utf-8

# ## College Football Data Spark Notebook
# 
# 
# 

# In[1]:


# College Football - Drive Stats
import requests
import json
import pandas as pd
from datetime import datetime 

#Spark Configuration
spark.conf.set("sprk.sql.parquet.vorder.enabled", "true")
spark.conf.set("spark.microsoft.delta.optimizeWrite.enabled", "true")
spark.conf.set("spark.microsoft.delta.optimizeWrite.binSize", "1073741824")

# Import the necessary Spark libraries
from pyspark.sql import SparkSession

# Define a list of years from 2004 (when the api has data) through current year
current_year = datetime.now().year + 1
starting_year = datetime.now().year - 19

years = list(range(starting_year, current_year))

# Initialize an empty list to store DataFrames
dfs = []

# Loop through each year
for year in years:
    url = f"https://api.collegefootballdata.com/drives?seasonType=regular&year={year}"

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

        # Define a function to extract minutes and seconds
        def extract_time(row, time_key, minutes_key, seconds_key):
            if time_key in row and 'minutes' in row[time_key] and 'seconds' in row[time_key]:
                row[minutes_key] = row[time_key]['minutes']
                row[seconds_key] = row[time_key]['seconds']
            return row  
    
        # Apply the function to create new columns
        df = df.apply(lambda row: extract_time(row, 'start_time', 'start_time_minutes', 'start_time_seconds'), axis=1)
        df = df.apply(lambda row: extract_time(row, 'end_time', 'end_time_minutes', 'end_time_seconds'), axis=1)
        df = df.apply(lambda row: extract_time(row, 'elapsed', 'elapsed_minutes', 'elapsed_seconds'), axis=1)

        # Drop the original columns
        df = df.drop(columns=['start_time', 'end_time', 'elapsed'])

        #Add a new 'year' column 
        df['year'] = year
        
        # Append the DataFrame to the list
        dfs.append(df)
    else:
        print(f"API request failed for year {year} with status code:", response.status_code)

# Concatenate all DataFrames into one
final_df = pd.concat(dfs, ignore_index=True)

#Writing data to the lakehouse
sparkdf = spark.createDataFrame(final_df)
sparkdf.write.mode("overwrite").format("csv").save("Files/CFB/OffensiveProduction")

table_name = 'CFB_DriveStats'
sparkdf.write.mode("overwrite").format("delta").save("Tables/" + table_name)


# In[2]:


#Win-Loss Records by Year
import requests, json
import pandas as pd
from datetime import datetime 

#Spark Configuration
spark.conf.set("sprk.sql.parquet.vorder.enabled", "true")
spark.conf.set("spark.microsoft.delta.optimizeWrite.enabled", "true")
spark.conf.set("spark.microsoft.delta.optimizeWrite.binSize", "1073741824")

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


# In[3]:


#Talent Rankings by Year
import requests, json
import pandas as pd
from datetime import datetime 

#Spark Configuration
spark.conf.set("sprk.sql.parquet.vorder.enabled", "true")
spark.conf.set("spark.microsoft.delta.optimizeWrite.enabled", "true")
spark.conf.set("spark.microsoft.delta.optimizeWrite.binSize", "1073741824")

# Define a list of years from 2004 (when the api has data) through current year
current_year = datetime.now().year + 1 
starting_year = datetime.now().year - 19


years = list(range(starting_year, current_year))

# Initialize an empty list to store DataFrames
dfs = []

# Loop through each year
for year in years:
    
    url = f"https://api.collegefootballdata.com/talent?year={year}"

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
        # Append the DataFrame to the list
        dfs.append(df)

    else:
        print(f"API request failed for year {year} with status code:", response.status_code)

# Concatenate all DataFrames into one
final_df = pd.concat(dfs, ignore_index=True)

#Writing data to the lakehouse
sparkdf = spark.createDataFrame(final_df)
sparkdf.write.mode("overwrite").format("csv").save("Files/CFB/TalentCompositeRankings")

table_name = 'CFB_TalentCompositeRankings'
sparkdf.write.mode("overwrite").format("delta").save("Tables/" + table_name)


# In[4]:


#Recruiting Rankings by Year
import requests, json
import pandas as pd
from datetime import datetime 

#Spark Configuration
spark.conf.set("sprk.sql.parquet.vorder.enabled", "true")
spark.conf.set("spark.microsoft.delta.optimizeWrite.enabled", "true")
spark.conf.set("spark.microsoft.delta.optimizeWrite.binSize", "1073741824")

# Define a list of years from 2004 (when the api has data) through current year
current_year = datetime.now().year + 1 
starting_year = datetime.now().year - 19


years = list(range(starting_year, current_year))

# Initialize an empty list to store DataFrames
dfs = []

# Loop through each year
for year in years:
    
    url = f"https://api.collegefootballdata.com/recruiting/teams?year={year}"

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
        # Append the DataFrame to the list
        dfs.append(df)

    else:
        print(f"API request failed for year {year} with status code:", response.status_code)

# Concatenate all DataFrames into one
final_df = pd.concat(dfs, ignore_index=True)

#Writing data to the lakehouse
sparkdf = spark.createDataFrame(final_df)
sparkdf.write.mode("overwrite").format("csv").save("Files/CFB/RecruitingRankings")

table_name = 'CFB_RecruitingRankings'
sparkdf.write.mode("overwrite").format("delta").save("Tables/" + table_name)


# In[5]:


#Season Stats by Year
import requests, json
import pandas as pd
from datetime import datetime 

#Spark Configuration
spark.conf.set("sprk.sql.parquet.vorder.enabled", "true")
spark.conf.set("spark.microsoft.delta.optimizeWrite.enabled", "true")
spark.conf.set("spark.microsoft.delta.optimizeWrite.binSize", "1073741824")

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
        'Authorization': 'Bearer <enter your API Key here>'
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


# In[14]:


#Advanced Season Stats by Year
import requests, json
import pandas as pd
from datetime import datetime 

#Spark Configuration
spark.conf.set("sprk.sql.parquet.vorder.enabled", "true")
spark.conf.set("spark.microsoft.delta.optimizeWrite.enabled", "true")
spark.conf.set("spark.microsoft.delta.optimizeWrite.binSize", "1073741824")

# Define a list of years from 2004 (when the api has data) through current year
current_year = datetime.now().year + 1 
starting_year = datetime.now().year - 19


years = list(range(starting_year, current_year))

# Initialize an empty list to store DataFrames
dfs = []

# Loop through each year
for year in years:
    
    url = f"https://api.collegefootballdata.com/stats/season/advanced?year={year}&excludeGarbageTime=true"

    payload = {}
    headers = {
        'accept': 'application/json',
        'Authorization': 'Bearer <enter your API Key here>'
    }

    response = requests.request("GET", url, headers=headers, data=payload)

    # Check if the request was successful
    if response.status_code == 200:
        # Parse the JSON response into a Python dictionary
        json_data = response.json()
        df = pd.json_normalize(json_data)
        # Append the DataFrame to the list
        dfs.append(df)

    else:
        print(f"API request failed for year {year} with status code:", response.status_code)

# Concatenate all DataFrames into one
final_df = pd.concat(dfs, ignore_index=True)

#Writing data to the lakehouse
sparkdf = spark.createDataFrame(final_df)

#Writing data to the lakehouse
sparkdf = spark.createDataFrame(final_df)
sparkdf.write.mode("overwrite").format("csv").save("Files/CFB/SeasonStats_Advanced")

table_name = 'CFB_SeasonStats_Advanced'
sparkdf.write.mode("overwrite").format("delta").save("Tables/" + table_name)


# **Machine Learning Code : Predicting Win Ratio**

# In[50]:


#Creating an integrated data set
from pyspark.sql.functions import col, expr, year, current_date
import pandas as pd 
from datetime import datetime 

CFB_WinLoss = spark.sql("SELECT * FROM artlakehouse01.CFB_WinLoss")
CFB_TalentCompositeRankings = spark.sql("SELECT * FROM artlakehouse01.CFB_TalentCompositeRankings")
CFB_RecruitingRankings = spark.sql("SELECT * FROM artlakehouse01.CFB_RecruitingRankings")
CFB_TalentCompositeRankings = spark.sql("SELECT * FROM artlakehouse01.CFB_TalentCompositeRankings")

CFB0 = spark.sql("SELECT * FROM artlakehouse01.CFB_SeasonStats")
pivot_df = CFB0.withColumnRenamed("statName", "stat_name").withColumnRenamed("statValue", "stat_value")

# Use PySpark pivot to transform rows into columns
pivot_df = cfb_df.groupBy("season", "team", "conference").pivot("stat_name").max("stat_value")
pivot_df = pivot_df.na.fill(0)

vw_CFB_SeasonStatsBasic = pivot_df.select(
    col("season").alias("year"),
    col("team").alias("school"),
    col("conference"),
    col("passAttempts").alias("passAttempts"),
    col("sacks").alias("sacks"),
    col("turnovers").alias("turnovers"),
    col("passesIntercepted").alias("passesIntercepted"),
    col("tacklesForLoss").alias("tacklesforLoss"),
    col("totalYards").alias("totalYards"),
    col("rushingYards").alias("rushingYards"),
    col("fumblesLost").alias("fumblesLost"),
    col("rushingAttempts").alias("rushingAttempts"),
    col("firstDowns").alias("firstDowns"),
    col("puntReturnYards").alias("puntReturnYards"),
    col("netPassingYards").alias("netPassingYards"),
    col("fourthDowns").alias("fourthDowns"),
    col("kickReturnTDs").alias("kickReturnTDs"),
    col("rushingTDs").alias("rushingTDs"),
    col("penalties").alias("penalties"),
    col("interceptions").alias("interceptions"),
    col("fourthDownConversions").alias("fourthDownConversions"),
    col("passingTDs").alias("passingTDs"),
    col("interceptionTDs").alias("interceptionTDs"),
    col("fumblesRecovered").alias("fumblesRecovered"),
    col("penaltyYards").alias("penaltyYards"),
    col("interceptionYards").alias("interceptionYards"),
    col("kickReturns").alias("kickReturns"),
    col("passCompletions").alias("passCompletions"),
    col("puntReturnTDs").alias("puntReturnTDs"),
    col("thirdDowns").alias("thirdDowns"),
    col("kickReturnYards").alias("kickReturnYards"),
    col("puntReturns").alias("puntReturns"),
    col("possessionTime").alias("possessionTime"),
    col("thirdDownConversions").alias("thirdDownConversions")
)

# Perform left joins
composite_results = CFB_WinLoss \
    .join(CFB_TalentCompositeRankings, (CFB_WinLoss["year"] == CFB_TalentCompositeRankings["year"]) & (CFB_WinLoss["team"] == CFB_TalentCompositeRankings["school"]), "left") \
    .join(CFB_RecruitingRankings, (CFB_WinLoss["year"] == CFB_RecruitingRankings["year"]) & (CFB_WinLoss["team"] == CFB_RecruitingRankings["team"]), "left") \
    .join(vw_CFB_SeasonStatsBasic, (CFB_WinLoss["year"] == vw_CFB_SeasonStatsBasic["year"]) & (CFB_WinLoss["team"] == vw_CFB_SeasonStatsBasic["school"]), "left")



# Select and cast columns
composite_results = composite_results.select(
    CFB_WinLoss["year"],
    CFB_WinLoss["team"].alias("school"),
    col("talent").cast("float").alias("CompositeTalent"),
    col("points").cast("float").alias("RecruitingPoints"),
    col("rank").alias("RecruitingRank"),
    CFB_WinLoss["teamId"],
    CFB_WinLoss["conference"],
    CFB_WinLoss["division"],
    CFB_WinLoss["expectedWins"],
    CFB_WinLoss["totalgames"],
    CFB_WinLoss["totalwins"],
    CFB_WinLoss["totalties"],
    CFB_WinLoss["conferencegames"],
    CFB_WinLoss["conferencewins"],
    CFB_WinLoss["conferencelosses"],
    CFB_WinLoss["conferenceties"],
    CFB_WinLoss["homegames"],
    CFB_WinLoss["homewins"],
    CFB_WinLoss["homelosses"],
    CFB_WinLoss["hometies"],
    CFB_WinLoss["awaygames"],
    CFB_WinLoss["awaywins"],
    CFB_WinLoss["awaylosses"],
    CFB_WinLoss["awayties"],
    vw_CFB_SeasonStatsBasic["firstDowns"],
    vw_CFB_SeasonStatsBasic["fourthDownConversions"],
    vw_CFB_SeasonStatsBasic["fourthDowns"],
    vw_CFB_SeasonStatsBasic["fumblesLost"],
    vw_CFB_SeasonStatsBasic["interceptionTDs"],
    vw_CFB_SeasonStatsBasic["interceptionYards"],
    vw_CFB_SeasonStatsBasic["interceptions"],
    vw_CFB_SeasonStatsBasic["kickReturnTDs"],
    vw_CFB_SeasonStatsBasic["kickReturnYards"],
    vw_CFB_SeasonStatsBasic["kickReturns"],
    vw_CFB_SeasonStatsBasic["netPassingYards"],
    vw_CFB_SeasonStatsBasic["passAttempts"],
    vw_CFB_SeasonStatsBasic["passCompletions"],
    vw_CFB_SeasonStatsBasic["passesIntercepted"],
    vw_CFB_SeasonStatsBasic["passingTDs"],
    vw_CFB_SeasonStatsBasic["penalties"],
    vw_CFB_SeasonStatsBasic["penaltyYards"],
    vw_CFB_SeasonStatsBasic["possessionTime"],
    vw_CFB_SeasonStatsBasic["puntReturnTDs"],
    vw_CFB_SeasonStatsBasic["puntReturns"],
    vw_CFB_SeasonStatsBasic["rushingAttempts"],
    vw_CFB_SeasonStatsBasic["rushingTDs"],
    vw_CFB_SeasonStatsBasic["rushingYards"],
    vw_CFB_SeasonStatsBasic["sacks"],
    vw_CFB_SeasonStatsBasic["tacklesforLoss"],
    vw_CFB_SeasonStatsBasic["thirdDownConversions"],
    vw_CFB_SeasonStatsBasic["thirdDowns"],
    vw_CFB_SeasonStatsBasic["totalYards"],
    vw_CFB_SeasonStatsBasic["turnovers"]
)

# Create new variables
cfb0 = composite_results.withColumn("WinRatio", col("totalwins") / col("totalgames")) \
    .withColumn("firstDownsPerGame", col("firstDowns") / col("totalgames")) \
    .withColumn("fourthDownRate", col("fourthDownConversions") / col("fourthDowns")) \
    .withColumn("interceptionYardsPerGame", col("interceptionYards") / col("totalgames")) \
    .withColumn("kickReturnYardsPerGame", col("kickReturnYards") / col("totalgames")) \
    .withColumn("passingYardsPerGame", col("netPassingYards") / col("totalgames")) \
    .withColumn("rushingYardsPerGame", col("rushingYards") / col("totalgames")) \
    .withColumn("rushingYardsAvg", col("rushingYards") / col("rushingAttempts")) \
    .withColumn("passingYardsAvg", col("netPassingYards") / col("passAttempts")) \
    .withColumn("passRate", col("passCompletions") / col("passAttempts")) \
    .withColumn("sacksperGame", col("sacks") / col("totalgames")) \
    .withColumn("tacklesperGame", col("tacklesforLoss") / col("totalgames")) \
    .withColumn("YardsPerGame", col("totalYards") / col("totalgames"))

# Select the desired columns
cfb0 = cfb0.select(
    "year", "school", "conference", "CompositeTalent", "RecruitingPoints",
    "WinRatio", "firstDownsPerGame", "fourthDownRate", "interceptionYardsPerGame",
    "kickReturnYardsPerGame", "passingYardsPerGame", "rushingYardsPerGame",
    "rushingYardsAvg", "passingYardsAvg", "passRate", "sacksperGame", "tacklesperGame",
    "YardsPerGame"
)


# define current year and prior year
prior_year = datetime.now().year - 1
cfb1 = cfb0.filter(cfb0["year"] < prior_year)
cfb_current = cfb0.filter(cfb0["year"] == prior_year)

pandasdf = cfb1.toPandas()
pandasdf = pandasdf.fillna(0)
pandasdf = pandasdf[pandasdf['CompositeTalent'] > 0]

df_priorYear = cfb_current.toPandas()
df_priorYear = df_priorYear.fillna(0)
df_priorYear = df_priorYear[df_priorYear['CompositeTalent'] > 0]


# In[51]:


import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_squared_error, r2_score
import matplotlib.pyplot as plt


target_column = "WinRatio"
feature_columns = [
    "CompositeTalent", "RecruitingPoints",
    "firstDownsPerGame", "fourthDownRate", "interceptionYardsPerGame",
    "kickReturnYardsPerGame", "passingYardsPerGame", "rushingYardsPerGame",
    "rushingYardsAvg", "passingYardsAvg", "passRate", "sacksperGame", "tacklesperGame",
    "YardsPerGame"
]

X = pandasdf[feature_columns]
y = pandasdf[target_column]

X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

scaler = StandardScaler()
X_train = scaler.fit_transform(X_train)
X_test = scaler.transform(X_test)

model = LinearRegression()
model.fit(X_train, y_train)

predictions = model.predict(X_test)

mse = mean_squared_error(y_test, predictions)
print(f"Mean Squared Error for Model is: {mse}")

#Verifying Model Run

X_priorYear = scaler.transform(df_priorYear[feature_columns])
predictions_priorYear = model.predict(X_priorYear)

actual_priorYear_winratio = df_priorYear[target_column]
mse_priorYear = mean_squared_error(actual_priorYear_winratio, predictions_priorYear)
r2_priorYear = r2_score(actual_priorYear_winratio, predictions_priorYear)

print(f"Mean Squared Error for priorYear: {mse_priorYear}")
print(f"R-squared (R2) for priorYear: {r2_priorYear}")

#Plotting Predictions

plt.scatter(actual_priorYear_winratio, predictions_priorYear)
plt.xlabel("Actual WinRatio 2022")
plt.ylabel("Predicted WinRatio 2022")
plt.title("Actual vs. Predicted WinRatio for 2022")
plt.show()

# Assuming you have already loaded and prepared your df_priorYear DataFrame for priorYear data
# Standardize the feature data for priorYear, if you haven't already
X_priorYear = scaler.transform(df_priorYear[feature_columns])

# Make predictions for priorYear
predictions_priorYear = model.predict(X_priorYear)

# Create a new DataFrame with school names and predicted WinRatio for priorYear
predicted_df_priorYear = pd.DataFrame({
    'School': df_priorYear['school'],  # Assuming the school names are in the 'school' column
    'Predicted_WinRatio_priorYear': predictions_priorYear
})

# Reset the index of the new DataFrame
predicted_df_priorYear.reset_index(drop=True, inplace=True)

# Merge df_priorYear and predicted_df_priorYear on the 'school' column
final_df = df_priorYear.merge(predicted_df_priorYear, left_on='school', right_on='School', how='left')

# Fill missing values in 'Predicted_WinRatio_priorYear' with 0
final_df['Predicted_WinRatio_priorYear'].fillna(0, inplace=True)

# Drop the 'School' column since it's a duplicate of 'school'
final_df.drop(columns=['School'], inplace=True)

#Writing data to the lakehouse
sparkdf = spark.createDataFrame(final_df)
sparkdf.printSchema()
sparkdf.write.mode("overwrite").format("csv").save("Files/CFB/WinRatioPrediction")

table_name = 'CFB_WinRatioPrediction'
sparkdf.write.mode("overwrite").format("delta").save("Tables/" + table_name)

