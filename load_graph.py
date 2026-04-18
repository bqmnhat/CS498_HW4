import pandas as pd
from neo4j import GraphDatabase

EXTERNAL_IP = "34.173.172.171" 

class TaxiLoader:
    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self.driver.close()

    def load_data(self, csv_file):
        df = pd.read_csv(csv_file)
        data = df.to_dict('records')

        with self.driver.session() as session:
            # Constraints
            print("Ensuring constraints exist...")
            session.run("CREATE CONSTRAINT IF NOT EXISTS FOR (d:Driver) REQUIRE d.driver_id IS UNIQUE")
            session.run("CREATE CONSTRAINT IF NOT EXISTS FOR (c:Company) REQUIRE c.name IS UNIQUE")
            session.run("CREATE CONSTRAINT IF NOT EXISTS FOR (a:Area) REQUIRE a.area_id IS UNIQUE")

            # Batch Loading
            query = """
            UNWIND $rows AS row
            MERGE (d:Driver {driver_id: toString(row.driver_id)})
            MERGE (c:Company {name: row.company})
            MERGE (a:Area {area_id: toInteger(row.dropoff_area)})
            MERGE (d)-[:WORKS_FOR]->(c)
            CREATE (d)-[:TRIP {
                trip_id: row.trip_id,
                fare: toFloat(row.fare),
                trip_seconds: toInteger(row.trip_seconds)
            }]->(a)
            """
            print(f"Starting batch upload of {len(data)} rows...")
            session.run(query, rows=data)
            print("Upload completed successfully!")

if __name__ == "__main__":
    loader = TaxiLoader(f"bolt://{EXTERNAL_IP}:7687", "neo4j", "123456789")
    loader.load_data("taxi_trips_clean.csv")
    loader.close()
    print("Finished!")