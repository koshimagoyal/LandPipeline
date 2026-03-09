import json
import os
import time

# This simulates Flink processing a stream and sinking it to a shared volume
def simulate_flink_sink():
    data = {
        "parcel_id": "P101",
        "news_event": "New Commercial Zoning Approved",
        "timestamp": time.time()
    }
    # Path shared via docker volumes
    file_path = "/opt/flink/data/flink_output.json"
    with open(file_path, "w") as f:
        json.dump(data, f)
    print(f"Flink Sink Successful: {file_path}")

if __name__ == "__main__":
    simulate_flink_sink()