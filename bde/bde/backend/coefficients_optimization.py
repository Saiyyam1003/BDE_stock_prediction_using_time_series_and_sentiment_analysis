import json
import pandas as pd
import numpy as np
from sklearn.linear_model import LinearRegression

# Load data from marketData.json
with open("market_data.json", "r") as file:
    data = json.load(file)

# Convert JSON data into a DataFrame
df = pd.DataFrame(data['data'])

# Ensure sentiment scores are normalized
def normalize_sentiment(sentiments):
    min_sent = np.min(sentiments)
    max_sent = np.max(sentiments)
    return (sentiments - min_sent) / (max_sent - min_sent) if max_sent > min_sent else sentiments

df['normalized_sentiment'] = normalize_sentiment(df['sentiment'].values)

# Prepare features (rawPrediction and normalized_sentiment) and target (price)
X = df[['rawPrediction', 'normalized_sentiment']].values
y = df['price'].values

# Fit a linear regression model to find optimal coefficients
model = LinearRegression()
model.fit(X, y)

# Retrieve coefficients
alpha, beta = model.coef_
intercept = model.intercept_

# Display the calculated coefficients
print(f"Optimal Coefficients:")
print(f"alpha (rawPrediction): {alpha}")
print(f"beta (normalized_sentiment): {beta}")
print(f"intercept: {intercept}")


