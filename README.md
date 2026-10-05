# Real-Time Taxi Monitoring

A streaming data pipeline that replays GPS traces from a taxi fleet, computes per-taxi metrics in real time and shows them on a live map dashboard.

**Stack:** Apache Kafka · Apache Flink (Java) · Redis · FastAPI · WebSockets · Leaflet · Docker Compose

## Architecture

```
 GPS trace files ──► Kafka producer ──► Kafka topic ──► Flink job ──► Redis ──► FastAPI ──► Browser dashboard
   (data/*.txt)        (Python)      "taxi-locations"    (Java)     (state)    (WebSocket)    (Leaflet map)
```

1. **Kafka producer** (`kafka_producer/`) reads raw GPS records (`taxi_id, timestamp, longitude, latitude`) and sends them as JSON to the `taxi-locations` topic in timestamp order. `producer.py` handles inputs larger than memory with an external merge sort: it writes sorted chunks to temp files, k-way merges them with a heap and drops duplicates on the way.
2. **Flink job** (`flink_job/`) keys the stream by taxi and keeps per-taxi state (last position, running distance, speed sum) to compute:
   - current speed from consecutive points (haversine distance / time delta)
   - total distance travelled and average speed
   - filtering of implausible points (speed > 200 km/h, gaps > 1 h, points more than 15 km from the city centre)
   - area alerts when a taxi moves more than 10 km from the centre
3. **Redis sink** writes the latest state per taxi (`taxi:{id}` hashes with a TTL), the set of active taxis, a running fleet-wide distance, and capped lists of recent location updates and alerts (speeding > 50 km/h, area violations). Writes are batched with Redis pipelines.
4. **Dashboard** (`dashboard/`): a FastAPI backend reads Redis using pooled connections and pipelined batch reads, then pushes taxi positions (every 2 s), fleet stats (5 s) and alerts (1 s) to the browser over WebSockets. `index.html` renders the fleet on a clustered Leaflet map, with live stats and an alert feed.

## Project Structure

```
docker/docker-compose.yml        Zookeeper, Kafka, Redis, Flink JobManager + 2 TaskManagers, producer, dashboard
kafka_producer/                  Python producers + Dockerfile + wait-for-kafka.sh
flink_job/                       Maven project: FlinkTaxiJob.java (metrics), RedisSink.java (Redis writer)
dashboard/                       FastAPI app (app.py), frontend (index.html), Dockerfile.fastapi
```

## Running It

Requirements: Docker, Docker Compose, Java 11+ and Maven.

```bash
# 1. Build the Flink job jar (mounted into the Flink containers from flink_job/target)
cd flink_job && mvn clean package && cd ..

# 2. Put the GPS trace files in kafka_producer/data/ (one .txt file per taxi, CSV rows:
#    taxi_id,timestamp,longitude,latitude). kafka_producer/taxi_data.csv shows the format.

# 3. Start the stack
cd docker && docker compose up --build

# 4. Submit the job in the Flink UI at http://localhost:8081 (or with `flink run`)
# 5. Open the dashboard at http://localhost:8000
```

### API

| Endpoint | Description |
|---|---|
| `GET /` | Live dashboard |
| `WS /ws` | Pushes `initial_data`, `taxi_update`, `stats_update`, `alerts_update` messages |
| `GET /api/taxi_data` | Latest state of every active taxi |
| `GET /api/stats` | Active taxi count, fleet distance, alert count |
| `GET /api/alerts` | 20 most recent alerts |
| `GET /api/health` | Health check (used by Docker) |

## Data

The pipeline is built for the T-Drive format (Beijing taxi GPS traces, one file per taxi). Distances and the alert zone are measured from a fixed centre point in Beijing (39.916 N, 116.397 E). The dataset itself is not included in this repository.
