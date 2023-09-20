from pyspark.sql.functions import col, expr, year, current_date
import pandas as pd 
from datetime import datetime 

CFB_WinLoss = spark.sql("SELECT * FROM artlakehouse01.CFB_WinLoss")
CFB_TalentCompositeRankings = spark.sql("SELECT * FROM artlakehouse01.CFB_TalentCompositeRankings")
CFB_RecruitingRankings = spark.sql("SELECT * FROM artlakehouse01.CFB_RecruitingRankings")
CFB_TalentCompositeRankings = spark.sql("SELECT * FROM artlakehouse01.CFB_TalentCompositeRankings")
CFB_SeasonStatsAdvanced = spark.sql("SELECT * FROM artlakehouse01.CFB_SeasonStats_Advanced")

# Loop through the columns and rename them without periods
for old_col_name in CFB_SeasonStatsAdvanced.columns:
    new_col_name = old_col_name.replace(".", "")
    CFB_SeasonStatsAdvanced = CFB_SeasonStatsAdvanced.withColumnRenamed(old_col_name, new_col_name)

CFB0 = spark.sql("SELECT * FROM artlakehouse01.CFB_SeasonStats")
pivot_df = CFB0.withColumnRenamed("statName", "stat_name").withColumnRenamed("statValue", "stat_value")

# Use PySpark pivot to transform rows into columns
pivot_df = pivot_df.groupBy("season", "team", "conference").pivot("stat_name").max("stat_value")
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
    .join(CFB_SeasonStatsAdvanced, (CFB_WinLoss["year"] == CFB_SeasonStatsAdvanced["season"]) & (CFB_WinLoss["team"] == CFB_SeasonStatsAdvanced["team"]), "left") \
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
    vw_CFB_SeasonStatsBasic["turnovers"],
    col("offenseppa").cast("double").alias("offenseppa"),
    col("offensesuccessRate").cast("double").alias("offensesuccessRate"),
    col("offenseexplosiveness").cast("double").alias("offenseexplosiveness"),
    col("defenseppa").cast("double").alias("defenseppa"),
    col("defensesuccessRate").cast("double").alias("defensesuccessRate"),
    col("defenseexplosiveness").cast("double").alias("defenseexplosiveness"),
    col("offensepointsPerOpportunity").cast("double").alias("offensepointsPerOpportunity"),
    col("defensepointsPerOpportunity").cast("double").alias("defensepointsPerOpportunity"),
    col("offensesecondLevelYards").cast("double").alias("offensesecondLevelYards"),
    col("offenseopenFieldYards").cast("double").alias("offenseopenFieldYards"), 
    col("offensestandardDownssuccessRate").cast("double").alias("offensestandardDownssuccessRate"), 
    col("offensepassingDownsppa").cast("double").alias("offensepassingDownsppa"), 
    col("offensepassingDownssuccessRate").cast("double").alias("offensepassingDownssuccessRate"), 
    col("offensepassingDownsexplosiveness").cast("double").alias("offensepassingDownsexplosiveness"), 
    col("offenserushingPlaysppa").cast("double").alias("offenserushingPlaysppa"), 
    col("offenserushingPlayssuccessRate").cast("double").alias("offenserushingPlayssuccessRate"), 
    col("offenserushingPlaysexplosiveness").cast("double").alias("offenserushingPlaysexplosiveness"), 
    col("defensepassingDownsrate").cast("double").alias("defensepassingDownsrate"), 
    col("defensepassingDownsppa").cast("double").alias("defensepassingDownsppa"), 
    col("defensepassingDownssuccessRate").cast("double").alias("defensepassingDownssuccessRate"), 
    col("defensepassingDownsexplosiveness").cast("double").alias("defensepassingDownsexplosiveness"), 
    col("defenserushingPlaysrate").cast("double").alias("defenserushingPlaysrate"), 
    col("defenserushingPlaysppa").cast("double").alias("defenserushingPlaysppa"), 
    col("defenserushingPlaystotalPPA").cast("double").alias("defenserushingPlaystotalPPA"), 
    col("defenserushingPlayssuccessRate").cast("double").alias("defenserushingPlayssuccessRate"), 
    col("defenserushingPlaysexplosiveness").cast("double").alias("defenserushingPlaysexplosiveness"), 
    col("defensepassingPlaysrate").cast("double").alias("defensepassingPlaysrate"), 
    col("defensepassingPlaysppa").cast("double").alias("defensepassingPlaysppa"), 
    col("defensepassingPlaystotalPPA").cast("double").alias("defensepassingPlaystotalPPA"), 
    col("defensepassingPlayssuccessRate").cast("double").alias("defensepassingPlayssuccessRate")
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
    "YardsPerGame",
    "offenseppa", "offensesuccessRate", "offenseexplosiveness", "offensepointsPerOpportunity", 
    "defenseppa", "defensesuccessRate", "defenseexplosiveness", "defensepointsPerOpportunity",
    "offensesecondLevelYards", "offenseopenFieldYards", "offensestandardDownssuccessRate", "offensepassingDownsppa",
    "offensepassingDownssuccessRate", "offensepassingDownsexplosiveness", "offenserushingPlaysppa", "offenserushingPlayssuccessRate",
    "offenserushingPlaysexplosiveness", "defensepassingDownsrate", "defensepassingDownsppa", "defensepassingDownssuccessRate",
    "defensepassingDownsexplosiveness", "defenserushingPlaysrate", "defenserushingPlaysppa", "defenserushingPlaystotalPPA",
    "defenserushingPlayssuccessRate", "defenserushingPlaysexplosiveness", "defensepassingPlaysrate", "defensepassingPlaysppa",
    "defensepassingPlaystotalPPA", "defensepassingPlayssuccessRate"
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

target_column = "WinRatio"
feature_columns = [
    "CompositeTalent", "RecruitingPoints",
    "firstDownsPerGame", "fourthDownRate", "interceptionYardsPerGame",
    "kickReturnYardsPerGame", "passingYardsPerGame", "rushingYardsPerGame",
    "rushingYardsAvg", "passingYardsAvg", "passRate", "sacksperGame", "tacklesperGame",
    "YardsPerGame",
     "offenseppa", "offensesuccessRate", "offenseexplosiveness", "offensepointsPerOpportunity", 
    "defenseppa", "defensesuccessRate", "defenseexplosiveness", "defensepointsPerOpportunity",
     "offensesecondLevelYards", "offenseopenFieldYards", "offensestandardDownssuccessRate", "offensepassingDownsppa",
    "offensepassingDownssuccessRate", "offensepassingDownsexplosiveness", "offenserushingPlaysppa", "offenserushingPlayssuccessRate",
    "offenserushingPlaysexplosiveness", "defensepassingDownsrate", "defensepassingDownsppa", "defensepassingDownssuccessRate",
    "defensepassingDownsexplosiveness", "defenserushingPlaysrate", "defenserushingPlaysppa", "defenserushingPlaystotalPPA",
    "defenserushingPlayssuccessRate", "defenserushingPlaysexplosiveness", "defensepassingPlaysrate", "defensepassingPlaysppa",
    "defensepassingPlaystotalPPA", "defensepassingPlayssuccessRate"
]

X = pandasdf[feature_columns]
y = pandasdf[target_column]

