import json
import psycopg2
from kafka import KafkaConsumer


def run_consumer():
    """Consumes messages from Kafka and inserts them into PostgreSQL."""
    try:
        print("[Consumer] Connecting to Kafka at localhost:9092...")
        consumer = KafkaConsumer(
            "trips",
            bootstrap_servers="localhost:9092",
            auto_offset_reset="earliest",
            enable_auto_commit=True,
            value_deserializer=lambda v: json.loads(v.decode("utf-8")),
            group_id="trips-consumer-group",
        )
        print("[Consumer] ✓ Connected to Kafka successfully!")

        print("[Consumer] Connecting to PostgreSQL...")
        conn = psycopg2.connect(
            dbname="kafka_db",
            user="kafka_user",
            password="kafka_password",
            host="localhost",
            port="5432",
        )
        conn.autocommit = True
        cur = conn.cursor()
        print("[Consumer] ✓ Connected to PostgreSQL successfully!")

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS trips (
                trip_id VARCHAR(50) PRIMARY KEY,
                status VARCHAR(50),
                vehicle_type VARCHAR(50),
                distance_km NUMERIC(6,2),
                fare NUMERIC(10,2),
                timestamp TIMESTAMP,
                city VARCHAR(100),
                payment_method VARCHAR(50),
                discount NUMERIC(4,2),
                passenger_rating NUMERIC(2,1),
                weather VARCHAR(50)
            );
            """
        )
        print("[Consumer] ✓ Table 'trips' ready.")
        print("[Consumer] 🎧 Listening for messages...\n")

        count = 0
        for message in consumer:
            try:
                trip_data = message.value
                insert_query = """
                    INSERT INTO trips (trip_id, status, vehicle_type, distance_km, fare, timestamp, city, payment_method, discount, passenger_rating, weather)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (trip_id) DO NOTHING;
                """
                cur.execute(
                    insert_query,
                    (
                        trip_data["trip_id"],
                        trip_data["status"],
                        trip_data["vehicle_type"],
                        trip_data["distance_km"],
                        trip_data["fare"],
                        trip_data["timestamp"],
                        trip_data.get("city", "N/A"),
                        trip_data["payment_method"],
                        trip_data["discount"],
                        trip_data["passenger_rating"],
                        trip_data["weather"],
                    ),
                )
                count += 1
                print(
                    f"[Consumer] ✓ #{count} Inserted trip {trip_data['trip_id']} | {trip_data['vehicle_type']} | ${trip_data['fare']} | {trip_data['city']}"
                )
            except Exception as e:
                print(f"[Consumer ERROR] Failed to process message: {e}")
                continue

    except Exception as e:
        print(f"[Consumer ERROR] {e}")
        import traceback

        traceback.print_exc()
        raise


if __name__ == "__main__":
    run_consumer()
