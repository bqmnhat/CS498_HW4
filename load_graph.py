import pandas as pd
from neo4j import GraphDatabase

class TaxiLoader:
    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self.driver.close()

    def load_data(self, csv_file):
        df = pd.read_csv(csv_file)
        with self.driver.session() as session:
            session.run("CREATE CONSTRAINT FOR (d:Driver) REQUIRE d.driver_id IS UNIQUE IF NOT EXISTS")
            session.run("CREATE CONSTRAINT FOR (c:Company) REQUIRE c.name IS UNIQUE IF NOT EXISTS")
            session.run("CREATE CONSTRAINT FOR (a:Area) REQUIRE a.area_id IS UNIQUE IF NOT EXISTS")

            for _, row in df.iterrows():
                session.execute_write(self._create_graph_elements, row)

    @staticmethod
    def _create_graph_elements(tx, row):
        query = """
            MERGE (d:Driver {driver_id: $driver_id})
            MERGE (c:Company {name: $company})
            MERGE (a:Area {area_id: $area_id})
            MERGE (d)-[:WORKS_FOR]->(c)
            CREATE (d)-[:TRIP {
                trip_id: $trip_id,
                fare: $fare,
                trip_seconds: $trip_seconds
            }]->(a)
        """
        tx.run(
                query, 
                driver_id=str(row['driver_id']),
                company=row['company'],
                area_id=int(row['dropoff_area']),
                trip_id=row['trip_id'],
                fare=float(row['fare']),
                trip_seconds=int(row['trip_seconds'])
            )

if __name__ == "__main__":
    loader = TaxiLoader("bolt://localhost:7687", "neo4j", "YOUR_PASSWORD")
    loader.load_data("taxi_trips_clean.csv")
    loader.close()
    print("Graph loading complete.")