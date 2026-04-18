import pandas as pd

df = pd.read_csv("taxi_trips.csv")

# Step 1: Keep only the columns used in this assignment
cols = ["Trip ID", "Taxi ID", "Company", "Pickup Community Area",
        "Dropoff Community Area", "Fare", "Trip Seconds"]
df = df[cols]

# Step 2: Rename columns to snake_case
df.columns = ["trip_id", "driver_id", "company", "pickup_area",
              "dropoff_area", "fare", "trip_seconds"]

# Step 3: Drop rows where any of these columns are null
df = df.dropna(subset=["trip_id", "driver_id", "company",
                        "pickup_area", "dropoff_area", "fare", "trip_seconds"])

# Step 4: Cast to correct types
df["pickup_area"]   = df["pickup_area"].astype(int)
df["dropoff_area"]  = df["dropoff_area"].astype(int)
df["fare"]          = df["fare"].astype(float)
df["trip_seconds"]  = df["trip_seconds"].astype(int)

# Step 5: Drop rows where fare <= 0 or trip_seconds <= 0
df = df[df["fare"] > 0]
df = df[df["trip_seconds"] > 0]

# Step 6: Take the first 10,000 rows AFTER cleaning
df = df.head(10000)

df.to_csv("taxi_trips_clean.csv", index=False)
print(f"Cleaned dataset: {len(df)} rows")