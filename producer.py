import time
import json
import uuid
import random
from datetime import datetime
from kafka import KafkaProducer
from faker import Faker

fake = Faker()


def generate_synthetic_trip():
    """Generates realistic synthetic ride-sharing trip data."""
    cities = [
        "New York",
        "Los Angeles",
        "Chicago",
        "Houston",
        "Miami",
        "San Francisco",
        "Seattle",
    ]
    vehicle_types = ["Economy", "Premium", "SUV", "Luxury"]
    payment_methods = ["Credit Card", "Debit Card", "PayPal", "Apple Pay", "Cash"]
    weather_conditions = ["Sunny", "Rainy", "Cloudy", "Stormy", "Snowy"]

    trip_distance = round(random.uniform(1, 50), 2)  # km
    base_fare = 2.5
    fare_per_km = {"Economy": 1.2, "Premium": 2.0, "SUV": 2.5, "Luxury": 3.5}
    vehicle_type = random.choice(vehicle_types)
    total_fare = base_fare + trip_distance * fare_per_km[vehicle_type]
    discount = random.choice([0, 0.05, 0.1, 0.15])
    net_fare = total_fare * (1 - discount)

    return {
        "trip_id": str(uuid.uuid4())[:8],
        "status": random.choice(["Completed", "Cancelled", "Ongoing"]),
        "vehicle_type": vehicle_type,
        "distance_km": trip_distance,
        "fare": round(net_fare, 2),
        "timestamp": datetime.now().isoformat(),
        "city": random.choice(cities),
        "payment_method": random.choice(payment_methods),
        "discount": round(discount, 2),
        "passenger_rating": round(random.uniform(3.5, 5.0), 1),
        "weather": random.choice(weather_conditions),
    }


def run_producer():
    """Kafka producer that sends synthetic ride trips to the 'trips' topic."""
    try:
        print("[Producer] Connecting to Kafka at localhost:9092...")
        producer = KafkaProducer(
            bootstrap_servers="localhost:9092",
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            request_timeout_ms=30000,
            max_block_ms=60000,
            retries=5,
        )
        print("[Producer] ✓ Connected to Kafka successfully!")

        count = 0
        while True:
            trip = generate_synthetic_trip()
            print(f"[Producer] Sending trip #{count}: {trip}")
            future = producer.send("trips", value=trip)
            record_metadata = future.get(timeout=10)
            print(
                f"[Producer] ✓ Sent to partition {record_metadata.partition} at offset {record_metadata.offset}"
            )
            producer.flush()
            count += 1
            time.sleep(random.uniform(0.5, 2.0))

    except Exception as e:
        print(f"[Producer ERROR] {e}")
        import traceback

        traceback.print_exc()
        raise


if __name__ == "__main__":
    run_producer()
