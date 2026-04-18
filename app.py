from flask import Flask, request, jsonify
from neo4j import GraphDatabase
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, count, round as spark_round

app = Flask(__name__)

# Neo4j Config
neo_driver = GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", "123456789"))

# Spark Config
spark = SparkSession.builder.appName("TaxiAPI").getOrCreate()

# NEO4J
@app.route('/graph-summary', methods=['GET'])
def graph_summary():
    with neo_driver.session() as session:
        driver_count = session.run("MATCH (n:Driver) RETURN count(n)").single()[0]
        company_count = session.run("MATCH (n:Company) RETURN count(n)").single()[0]
        area_count = session.run("MATCH (n:Area) RETURN count(n)").single()[0]
        trip_count = session.run("MATCH ()-[r:TRIP]->() RETURN count(r)").single()[0]
    return jsonify({
        "driver_count": driver_count,
        "company_count": company_count,
        "area_count": area_count,
        "trip_count": trip_count
    })

@app.route('/top-companies', methods=['GET'])
def top_companies():
    n = int(request.args.get('n', 5))
    query = """
    MATCH (c:Company)<-[:WORKS_FOR]-(d:Driver)-[:TRIP]->()
    RETURN c.name as name, count(*) as trip_count
    ORDER BY trip_count DESC LIMIT $n
    """
    with neo_driver.session() as session:
        result = session.run(query, n=n)
        companies = [{"name": r["name"], "trip_count": r["trip_count"]} for r in result]
    return jsonify({"companies": companies})

@app.route('/high-fare-trips', methods=['GET'])
def high_fare_trips():
    area_id = int(request.args.get('area_id'))
    min_fare = float(request.args.get('min_fare'))
    query = """
    MATCH (d:Driver)-[r:TRIP]->(a:Area {area_id: $area_id})
    WHERE r.fare > $min_fare
    RETURN r.trip_id as trip_id, r.fare as fare, d.driver_id as driver_id
    ORDER BY fare DESC
    """
    with neo_driver.session() as session:
        result = session.run(query, area_id=area_id, min_fare=min_fare)
        trips = [{"trip_id": r["trip_id"], "fare": r["fare"], "driver_id": r["driver_id"]} for r in result]
    return jsonify({"trips": trips})

@app.route('/co-area-drivers', methods=['GET'])
def co_area_drivers():
    driver_id = request.args.get('driver_id')
    query = """
    MATCH (d1:Driver {driver_id: $driver_id})-[:TRIP]->(a:Area)<-[:TRIP]-(d2:Driver)
    WHERE d1 <> d2
    RETURN d2.driver_id as driver_id, count(DISTINCT a) as shared_areas
    ORDER BY shared_areas DESC
    """
    with neo_driver.session() as session:
        result = session.run(query, driver_id=driver_id)
        drivers = [{"driver_id": r["driver_id"], "shared_areas": r["shared_areas"]} for r in result]
    return jsonify({"co_area_drivers": drivers})

@app.route('/avg-fare-by-company', methods=['GET'])
def avg_fare_neo():
    query = """
    MATCH (c:Company)<-[:WORKS_FOR]-(d:Driver)-[r:TRIP]->()
    RETURN c.name as name, round(avg(r.fare), 2) as avg_fare
    ORDER BY avg_fare DESC
    """
    with neo_driver.session() as session:
        result = session.run(query)
        companies = [{"name": r["name"], "avg_fare": r["avg_fare"]} for r in result]
    return jsonify({"companies": companies})

# PYSPARK
@app.route('/area-stats', methods=['GET'])
def area_stats():
    area_id = int(request.args.get('area_id'))
    df = spark.read.csv("taxi_trips_clean.csv", header=True, inferSchema=True)
    stats = df.filter(col("dropoff_area") == area_id).agg(
        count("*").alias("trip_count"),
        avg("fare").alias("avg_fare"),
        avg("trip_seconds").alias("avg_trip_seconds")
    ).collect()[0]
    
    return jsonify({
        "area_id": area_id,
        "trip_count": stats["trip_count"],
        "avg_fare": round(stats["avg_fare"], 2) if stats["avg_fare"] else 0,
        "avg_trip_seconds": int(stats["avg_trip_seconds"]) if stats["avg_trip_seconds"] else 0
    })

@app.route('/top-pickup-areas', methods=['GET'])
def top_pickup_areas():
    n = int(request.args.get('n', 5))
    df = spark.read.csv("taxi_trips_clean.csv", header=True, inferSchema=True)
    top_areas = df.groupBy("pickup_area").count().orderBy(col("count").desc()).limit(n).collect()
    
    return jsonify({"areas": [{"pickup_area": r["pickup_area"], "trip_count": r["count"]} for r in top_areas]})

@app.route('/company-compare', methods=['GET'])
def company_compare():
    c1 = request.args.get('company1')
    c2 = request.args.get('company2')
    
    df = spark.read.csv("taxi_trips_clean.csv", header=True, inferSchema=True)
    df = df.withColumn("fare_per_minute", col("fare") / (col("trip_seconds") / 60.0))
    df.createOrReplaceTempView("compare_trips")
    
    results = spark.sql(f"""
        SELECT company, COUNT(*) as trip_count, ROUND(AVG(fare), 2) as avg_fare,
               ROUND(AVG(fare_per_minute), 2) as avg_fare_per_minute,
               ROUND(AVG(trip_seconds), 0) as avg_trip_seconds
        FROM compare_trips
        WHERE company IN ('{c1}', '{c2}')
        GROUP BY company
    """).collect()
    
    if len(results) < 1:
        return jsonify({"error": "one or more companies not found"}), 404
        
    return jsonify({"comparison": [dict(r) for r in results]})

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8000)