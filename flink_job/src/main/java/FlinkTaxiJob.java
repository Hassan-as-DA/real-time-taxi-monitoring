import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import redis.clients.jedis.Jedis;
import com.google.gson.Gson;


import java.time.LocalDateTime;
import java.time.Duration;
import java.time.format.DateTimeFormatter;
import java.util.*;

public class FlinkTaxiJob {

    private static final DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    private static final double FC_LAT = 39.916, FC_LON = 116.397;

    public static void main(String[] args) throws Exception {

        // 🧹 Clean Redis before Flink job starts
        try (Jedis jedis = new Jedis("redis", 6379)) {
            Set<String> keys = jedis.keys("taxi:*");
            for (String key : keys) jedis.del(key);
            jedis.del("alerts", "location_updates", "active_taxis", "fleet_total_distance");
            System.out.println("🧹 Redis cleaned before Flink job starts");
        }

        // ⚙️ Flink setup
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "kafka:9092");
        props.setProperty("group.id", "flink-taxi-replay-" + UUID.randomUUID());
        props.setProperty("auto.offset.reset", "earliest");

        // 🌊 Kafka stream
        DataStream<String> kafkaStream = env.addSource(
                new FlinkKafkaConsumer<>("taxi-locations", new SimpleStringSchema(), props));

        DataStream<TaxiLocation> locations = kafkaStream.process(new ProcessFunction<String, TaxiLocation>() {
            @Override
            public void processElement(String value, Context ctx, Collector<TaxiLocation> out) {
                try {
                    TaxiLocation loc = new Gson().fromJson(value, TaxiLocation.class);
                    out.collect(loc);
                } catch (Exception e) {
                    System.err.println(" Invalid message skipped: " + value);
                }
            }
        });

        locations
                .keyBy(loc -> loc.taxi_id)
                .map(new EnrichedTaxiMetrics()).name("Enrich Metrics")
                .addSink(new RedisSink()).name("Redis Sink");

        env.execute("Real-Time Taxi Stream Processor");
    }

    public static class TaxiLocation {
        public String taxi_id;
        public String timestamp;
        public double longitude;
        public double latitude;
    }

    public static class TaxiMetrics {
        public String taxi_id;
        public double current_speed;
        public double total_distance;
        public double average_speed;
        public double latitude;
        public double longitude;
        public String timestamp;
        public boolean should_emit_location;
        public boolean area_alert;
        public boolean discard;
    }

    public static class EnrichedTaxiMetrics extends RichMapFunction<TaxiLocation, TaxiMetrics> {
        private transient ValueState<TaxiLocation> lastLocation;
        private transient ValueState<Tuple2<Double, Integer>> speedState;
        private transient ValueState<Double> distanceState;
        private transient ValueState<LocalDateTime> lastEmitTime;

        @Override
        public void open(Configuration parameters) {
            lastLocation = getRuntimeContext().getState(
                    new ValueStateDescriptor<>("lastLocation", TaxiLocation.class));
            speedState = getRuntimeContext().getState(
                    new ValueStateDescriptor<>("speedState", TypeInformation.of(new TypeHint<Tuple2<Double, Integer>>() {})));
            distanceState = getRuntimeContext().getState(
                    new ValueStateDescriptor<>("distanceState", Double.class));
            lastEmitTime = getRuntimeContext().getState(
                    new ValueStateDescriptor<>("lastEmitTime", LocalDateTime.class));
        }

        @Override
        public TaxiMetrics map(TaxiLocation loc) throws Exception {
            TaxiLocation last = lastLocation.value();
            double speed = 0.0;
            double totalDistance = distanceState.value() != null ? distanceState.value() : 0.0;
            Tuple2<Double, Integer> speedSum = speedState.value() != null ? speedState.value() : Tuple2.of(0.0, 0);
            LocalDateTime emitCheckpoint = lastEmitTime.value();

            boolean shouldEmit = false, areaAlert = false, discard = false;
            LocalDateTime now = LocalDateTime.parse(loc.timestamp, formatter);

            if (last != null) {
                double d = haversine(last.latitude, last.longitude, loc.latitude, loc.longitude);
                long timeDiff = Duration.between(LocalDateTime.parse(last.timestamp, formatter), now).getSeconds();

                if (timeDiff > 0 && timeDiff < 3600) {
                    speed = d * 3600 / timeDiff;
                    if (speed > 200) {
                        discard = true;
                    } else {
                        totalDistance += d;
                        speedSum = Tuple2.of(speedSum.f0 + speed, speedSum.f1 + 1);
                    }
                } else {
                    discard = true;
                }
            }

            double distanceFromFC = haversine(loc.latitude, loc.longitude, FC_LAT, FC_LON);
            if (distanceFromFC > 15) discard = true;
            else if (distanceFromFC > 10) areaAlert = true;

            if (emitCheckpoint == null || Duration.between(emitCheckpoint, now).getSeconds() >= 5) {
                shouldEmit = true;
                lastEmitTime.update(now);
            }

            lastLocation.update(loc);
            distanceState.update(totalDistance);
            speedState.update(speedSum);

            TaxiMetrics m = new TaxiMetrics();
            m.taxi_id = loc.taxi_id;
            m.current_speed = speed;
            m.latitude = loc.latitude;
            m.longitude = loc.longitude;
            m.timestamp = loc.timestamp;
            m.total_distance = totalDistance;
            m.average_speed = speedSum.f1 > 0 ? speedSum.f0 / speedSum.f1 : 0.0;
            m.should_emit_location = shouldEmit;
            m.area_alert = areaAlert;
            m.discard = discard;

            return m;
        }

        private double haversine(double lat1, double lon1, double lat2, double lon2) {
            double R = 6371;
            double dLat = Math.toRadians(lat2 - lat1);
            double dLon = Math.toRadians(lon2 - lon1);
            double a = Math.sin(dLat / 2) * Math.sin(dLat / 2)
                    + Math.cos(Math.toRadians(lat1)) * Math.cos(Math.toRadians(lat2))
                    * Math.sin(dLon / 2) * Math.sin(dLon / 2);
            return R * 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
        }
    }

    
}
