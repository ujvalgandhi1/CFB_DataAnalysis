# CFB_DataAnalysis
Common Repository around College Football Data Analysis

I have been playing a lot with Microsoft Fabric (https://www.microsoft.com/en-us/microsoft-fabric) recently and within that, the Spark Compute. The Spark engine within Fabric is fast (really fast) and a fun and quick way of putting together data artificats 

I decided to put together some data analysis around College Football and Fabric to work together to create some magic

The first step was to head over to https://collegefootballdata.com/ and sign up for an API Key (https://collegefootballdata.com/key). The good folks at this website do all the hard work in collecting data around College Football and providing an excellent and easy way to gather data 

I would also advise you to get an account on Postman to fire up the API calls because it will make your life much easier. 

Once you have your Fabric workspace set up, follow the instructions at https://learn.microsoft.com/en-us/fabric/onelake/create-lakehouse-onelake and get a Lakehouse
Then Launch a Notebook and feel free to use the code that I have at https://github.com/ujvalgandhi1/CFB_DataAnalysis/blob/main/Code/College%20Football%20Data%20Spark%20Notebook.py and modify it as per your requirements. 

**Note** You dont need Fabric per se. This Python code with some modifications will run locally also. It uses a Spark Distribution but you can move the files over to a Pandas data frame and the code runs perfectly fine. 
