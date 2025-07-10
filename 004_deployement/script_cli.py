import argparse
import pandas as pd
import numpy as np
import pickle

categorical = ['PULocationID', 'DOLocationID']

def read_data(filename):
    df = pd.read_parquet(filename)
    
    # Calculate trip duration in minutes
    df['duration'] = df.tpep_dropoff_datetime - df.tpep_pickup_datetime
    df['duration'] = df.duration.dt.total_seconds() / 60
    
    # Filter durations between 1 and 60 minutes
    df = df[(df.duration >= 1) & (df.duration <= 60)].copy()
    
    # Handle missing categorical values and convert to string
    df[categorical] = df[categorical].fillna(-1).astype('int').astype('str')
    
    return df

def main(year, month):
    filename = f'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{year}-{month:02d}.parquet'
    df = read_data(filename)
    
    # Load model and DictVectorizer from pickle file inside the container
    with open('model.bin', 'rb') as f_in:
        dv, model = pickle.load(f_in)
    
    # Prepare data for prediction
    dicts = df[categorical].to_dict(orient='records')
    X_val = dv.transform(dicts)
    
    # Predict durations
    y_pred = model.predict(X_val)
    
    # Calculate and print mean predicted duration
    mean_duration = np.mean(y_pred)
    print(f"Mean predicted duration for {month:02d}/{year}: {mean_duration:.2f} minutes")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("year", type=int, help="Year")
    parser.add_argument("month", type=int, help="Month")
    args = parser.parse_args()
    main(args.year, args.month)
