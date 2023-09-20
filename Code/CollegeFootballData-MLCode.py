# Additonal ML Algorithms - Code 1
import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler
from sklearn.linear_model import LinearRegression, Lasso, ElasticNet
from sklearn.ensemble import RandomForestRegressor, GradientBoostingRegressor
from sklearn.svm import SVR
from xgboost import XGBRegressor
from sklearn.neural_network import MLPRegressor
from sklearn.metrics import mean_squared_error, r2_score

# Define a list of algorithms with their names - Code 1
algorithms = [
    ("Linear Regression", LinearRegression()),
    ("Lasso Regression", Lasso(alpha=0.1)),
    ("ElasticNet Regression", ElasticNet(alpha=0.1, l1_ratio=0.5)),
    ("Random Forest Regressor", RandomForestRegressor(n_estimators=100, random_state=42)),
    ("Gradient Boosting Regressor", GradientBoostingRegressor(n_estimators=100, random_state=42)),
    ("Support Vector Machine (SVM) Regressor", SVR(kernel='linear')),
    ("XGBoost Regressor", XGBRegressor(n_estimators=100, random_state=42)),
    ("Neural Network (MLP) Regressor", MLPRegressor(hidden_layer_sizes=(100, 50), max_iter=1000, random_state=42))
]

# Split the data into training and testing sets - Code 1
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

# Initialize an empty dictionary to store the results - Code 1
results = {}

# Initialize variables to track the best model and its R2 score
best_model = None
best_r2 = -float('inf')

# Iterate through each algorithm, train, evaluate, and store the results
for algorithm_name, model in algorithms:
    # Standardize the data - Code 1
    scaler = StandardScaler()
    X_train_std = scaler.fit_transform(X_train)
    X_test_std = scaler.transform(X_test)
    
    # Train the model - Code 1
    model.fit(X_train_std, y_train)
    
    # Make predictions - Code 1
    predictions = model.predict(X_test_std)
    
    # Calculate metrics - Code 1
    mse = mean_squared_error(y_test, predictions)
    r2 = r2_score(y_test, predictions)
    
    # Store the results - Code 1
    results[algorithm_name] = {"Mean Squared Error": mse, "R-squared (R2)": r2}
    
    # Check if this model has the highest R2 so far
    if r2 > best_r2:
        best_r2 = r2
        best_model = model

# Find the algorithm with the highest R-squared (R2) - Code 1
best_algorithm_name = max(results, key=lambda k: results[k]["R-squared (R2)"])
print(f"The best-performing algorithm is {best_algorithm_name} with R2 = {results[best_algorithm_name]['R-squared (R2)']}")

X_priorYear = scaler.transform(df_priorYear[feature_columns])

predictions_priorYear = best_model.predict(X_priorYear)

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

X_priorYear = scaler.transform(df_priorYear[feature_columns])

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
sparkdf.write.mode("overwrite").format("csv").save("Files/CFB/WinRatioPrediction")

table_name = 'CFB_WinRatioPrediction'
sparkdf.write.mode("overwrite").format("delta").save("Tables/" + table_name)
