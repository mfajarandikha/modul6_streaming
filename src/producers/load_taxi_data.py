import csv
import json
from kafka import KafkaProducer
from time import time


def main():
    # Create a Kafka producer
    producer = KafkaProducer(
        bootstrap_servers="localhost:9092",
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )

    csv_file = "data/green_tripdata_2019-10.csv"  # Update path if needed

    with open(csv_file, "r", newline="", encoding="utf-8") as file:
        reader = csv.DictReader(file)

        for row in reader:
            try:
                # Convert numeric values to the correct type
                filtered_row = {
                    "lpep_pickup_datetime": row["lpep_pickup_datetime"],
                    "lpep_dropoff_datetime": row["lpep_dropoff_datetime"],
                    "PULocationID": (
                        int(row["PULocationID"]) if row["PULocationID"] else None
                    ),
                    "DOLocationID": (
                        int(row["DOLocationID"]) if row["DOLocationID"] else None
                    ),
                    "passenger_count": (
                        int(row["passenger_count"]) if row["passenger_count"] else 0
                    ),
                    "trip_distance": (
                        float(row["trip_distance"]) if row["trip_distance"] else 0.0
                    ),
                    "tip_amount": (
                        float(row["tip_amount"]) if row["tip_amount"] else 0.0
                    ),
                }

                # Send data to Kafka topic "green-trips"
                producer.send("green-trips", value=filtered_row)

            except ValueError as e:
                print(f"Skipping row due to error: {e}")

    # Make sure any remaining messages are delivered
    producer.flush()
    producer.close()


if __name__ == "__main__":
    t0 = time()
    main()
    t1 = time()
    took = t1 - t0
    print(f"Took {took:.2f} seconds")
