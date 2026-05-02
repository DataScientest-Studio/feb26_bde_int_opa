import pandas as pd
import psycopg2
import logging
import os
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import classification_report, confusion_matrix
import joblib


############# read in the transformed historical data with explanatory variables from the SQL database #############
DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "port": os.getenv("DB_PORT"),
    "dbname": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD")
}

conn = psycopg2.connect(connect_timeout=5, **DB_CONFIG)
conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)

logging.info("Connected to database")

cur = conn.cursor() 
feats = pd.read_sql_query("SELECT * FROM historical_klines_15m_features", conn)
logging.info("Read transformed historical data with features from database")

cur.close()
conn.close()
logging.info("Closed database connection")

############# prepare data #############

feats['open_time'] = pd.to_datetime(feats['open_time'], utc=True)
feats = feats.set_index('open_time')

Y = feats['trade_decision']  # Assuming 'trade_decision' is the target variable in the database
X = feats.drop(columns=['future_close', 'future_return','trade_decision'])  # Drop future info from features

# dropping NaNs
X = X.dropna()
Y = Y[X.index]  # Align target variable with features after dropping NaNs
logging.info("Prepared data for training")

# Train-test split
X_train, X_test, Y_train, Y_test = train_test_split(X, Y, test_size=0.2, random_state=42)

# feature scaling
scaler = StandardScaler()
X_train_scaled = scaler.fit_transform(X_train)
X_test_scaled = scaler.transform(X_test)    


############## Train a machine learning model (Random Forest) to predict trade decisions ##############
rf_clf = RandomForestClassifier(n_estimators=100, random_state=42)
rf_clf.fit(X_train_scaled, Y_train)
logging.info("Trained Random Forest model")

############## Evaluate the model ##############
logging.info("Evaluating model performance")
Y_pred = rf_clf.predict(X_test_scaled)
print("Classification Report:")
print(classification_report(Y_test, Y_pred))
print("Confusion Matrix:")
print(confusion_matrix(Y_test, Y_pred))
print(pd.crosstab(Y_test, Y_pred, rownames=['Actual'], colnames=['Predicted']))


############## Save the trained model and scaler for later use in prediction ##############
joblib.dump(rf_clf, '/models/rf_trade_decision_model.joblib')
joblib.dump(scaler, '/models/scaler.joblib')
logging.info("Saved trained model and scaler to disk")
