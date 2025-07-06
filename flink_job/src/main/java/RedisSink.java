import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.exceptions.JedisException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.gson.Gson;

import java.util.HashMap;
import java.util.Map;

public class ImprovedRedisSink extends RichSinkFunction<FlinkTaxiJob.TaxiMetrics> {
    
    private static final Logger LOG = LoggerFactory.getLogger(ImprovedRedisSink.class);
    private transient JedisPool jedisPool;
    private transient Gson gson;
    private Map<String, Double> lastDistances = new HashMap<>();
    
    // Alert data classes for structured storage
    public static class Alert {
        public String type;
        public String taxi_id;
        public String timestamp;
        public String message;
        public double speed; // For speeding alerts
        
        public Alert(String type, String taxi_id, String timestamp, String message) {
            this.type = type;
            this.taxi_id = taxi_id;
            this.timestamp = timestamp;
            this.message = message;
        }
        
        public Alert(String type, String taxi_id, String timestamp, String message, double speed) {
            this(type, taxi_id, timestamp, message);
            this.speed = speed;
        }
    }
    
    @Override
    public void open(Configuration parameters) {
        try {
            // Optimized connection pool configuration
            JedisPoolConfig config = new JedisPoolConfig();
            config.setMaxTotal(20);           // Increased pool size
            config.setMaxIdle(10);
            config.setMinIdle(2);
            config.setTestOnBorrow(true);
            config.setTestOnReturn(true);
            config.setTestWhileIdle(true);
            config.setMinEvictableIdleTimeMillis(60000);
            config.setTimeBetweenEvictionRunsMillis(30000);
            config.setNumTestsPerEvictionRun(3);
            config.setBlockWhenExhausted(true);
            config.setMaxWaitMillis(3000);
            
            jedisPool = new JedisPool(config, "redis", 6379, 5000); // 5 second timeout
            gson = new Gson();
            
            LOG.info("✅ Improved Redis connection pool initialized successfully");
        } catch (Exception e) {
            LOG.error("❌ Failed to initialize Redis connection pool", e);
            throw new RuntimeException("Could not initialize Redis connection", e);
        }
    }

    @Override
    public void invoke(FlinkTaxiJob.TaxiMetrics metrics, Context context) {
        if (metrics.discard) return;
        
        try (Jedis jedis = jedisPool.getResource()) {
            // Use pipeline for better performance
            var pipeline = jedis.pipelined();
            
            // Update taxi data efficiently
            updateTaxiDataOptimized(pipeline, metrics);
            
            // Handle location updates
            handleLocationUpdates(pipeline, metrics);
            
            // Handle alerts with structured format
            handleAlertsOptimized(pipeline, metrics);
            
            // Update fleet metrics correctly
            updateFleetMetricsOptimized(jedis, metrics);
            
            // Execute all pipeline operations
            pipeline.sync();
            
        } catch (JedisException e) {
            LOG.error("Redis operation failed for taxi {}", metrics.taxi_id, e);
            // Could implement retry logic or dead letter queue here
        } catch (Exception e) {
            LOG.error("Unexpected error processing metrics for taxi {}", metrics.taxi_id, e);
        }
    }
    
    private void updateTaxiDataOptimized(redis.clients.jedis.Pipeline pipeline, FlinkTaxiJob.TaxiMetrics metrics) {
        String key = "taxi:" + metrics.taxi_id;
        
        // Use pipeline for batch operations
        pipeline.hset(key, "taxi_id", metrics.taxi_id);
        pipeline.hset(key, "latitude", String.valueOf(metrics.latitude));
        pipeline.hset(key, "longitude", String.valueOf(metrics.longitude));
        pipeline.hset(key, "timestamp", metrics.timestamp);
        pipeline.hset(key, "speed", String.format("%.2f", metrics.current_speed));
        pipeline.hset(key, "avg_speed", String.format("%.2f", metrics.average_speed));
        pipeline.hset(key, "total_distance", String.format("%.2f", metrics.total_distance));
        pipeline.hset(key, "last_updated", String.valueOf(System.currentTimeMillis()));
        
        // Set expiration to clean up inactive taxis (30 minutes)
        pipeline.expire(key, 1800);
        
        // Add to active taxis set
        pipeline.sadd("active_taxis", metrics.taxi_id);
    }
    
    private void handleLocationUpdates(redis.clients.jedis.Pipeline pipeline, FlinkTaxiJob.TaxiMetrics metrics) {
        if (metrics.should_emit_location) {
            // Store as structured JSON for better processing
            Map<String, Object> locationUpdate = new HashMap<>();
            locationUpdate.put("taxi_id", metrics.taxi_id);
            locationUpdate.put("latitude", metrics.latitude);
            locationUpdate.put("longitude", metrics.longitude);
            locationUpdate.put("timestamp", metrics.timestamp);
            locationUpdate.put("speed", metrics.current_speed);
            
            String locationJson = gson.toJson(locationUpdate);
            pipeline.lpush("location_updates", locationJson);
            
            // Keep only recent location updates (last 500)
            pipeline.ltrim("location_updates", 0, 499);
        }
    }
    
    private void handleAlertsOptimized(redis.clients.jedis.Pipeline pipeline, FlinkTaxiJob.TaxiMetrics metrics) {
        // Handle speeding alerts with structured data
        if (metrics.current_speed > 50) {
            Alert speedingAlert = new Alert(
                "speeding",
                metrics.taxi_id,
                metrics.timestamp,
                String.format("Taxi %s speeding at %.2f km/h", metrics.taxi_id, metrics.current_speed),
                metrics.current_speed
            );
            
            String alertJson = gson.toJson(speedingAlert);
            pipeline.lpush("alerts", alertJson);
        }
        
        // Handle area violation alerts
        if (metrics.area_alert) {
            Alert areaAlert = new Alert(
                "area_violation",
                metrics.taxi_id,
                metrics.timestamp,
                String.format("Taxi %s violated area boundaries", metrics.taxi_id)
            );
            
            String alertJson = gson.toJson(areaAlert);
            pipeline.lpush("alerts", alertJson);
        }
        
        // Keep only recent alerts (last 200)
        if (metrics.current_speed > 50 || metrics.area_alert) {
            pipeline.ltrim("alerts", 0, 199);
        }
    }
    
    private void updateFleetMetricsOptimized(Jedis jedis, FlinkTaxiJob.TaxiMetrics metrics) {
        // Correct fleet distance calculation - only add incremental distance
        double previousDistance = lastDistances.getOrDefault(metrics.taxi_id, 0.0);
        double distanceIncrement = metrics.total_distance - previousDistance;
        
        // Only update if there's a positive increment (avoid issues with reprocessing)
        if (distanceIncrement > 0 && distanceIncrement < 50) { // Sanity check: max 50km increment
            try {
                Double currentTotal = jedis.incrByFloat("fleet_total_distance", distanceIncrement);
                LOG.debug("Fleet distance updated by {} km for taxi {}, new total: {} km", 
                         distanceIncrement, metrics.taxi_id, currentTotal);
            } catch (Exception e) {
                LOG.warn("Failed to update fleet distance for taxi {}: {}", metrics.taxi_id, e.getMessage());
            }
        }
        
        // Update our tracking
        lastDistances.put(metrics.taxi_id, metrics.total_distance);
        
        // Clean up inactive taxis periodically (every 100 invocations)
        if (System.currentTimeMillis() % 100 == 0) {
            cleanupInactiveTaxis(jedis);
        }
    }
    
    private void cleanupInactiveTaxis(Jedis jedis) {
        try {
            long currentTime = System.currentTimeMillis();
            long inactiveThreshold = currentTime - 600000; // 10 minutes
            
            // Get all active taxis
            var activeTaxis = jedis.smembers("active_taxis");
            int removedCount = 0;
            
            for (String taxiId : activeTaxis) {
                String key = "taxi:" + taxiId;
                String lastUpdated = jedis.hget(key, "last_updated");
                
                if (lastUpdated != null) {
                    try {
                        long lastUpdateTime = Long.parseLong(lastUpdated);
                        if (lastUpdateTime < inactiveThreshold) {
                            // Remove from active set
                            jedis.srem("active_taxis", taxiId);
                            // Redis TTL will clean up the taxi data automatically
                            lastDistances.remove(taxiId);
                            removedCount++;
                        }
                    } catch (NumberFormatException e) {
                        LOG.warn("Invalid timestamp for taxi {}: {}", taxiId, lastUpdated);
                    }
                }
            }
            
            if (removedCount > 0) {
                LOG.info("🗑️ Cleaned up {} inactive taxis", removedCount);
            }
            
        } catch (Exception e) {
            LOG.warn("Error during cleanup of inactive taxis", e);
        }
    }
    
    @Override
    public void close() throws Exception {
        if (jedisPool != null) {
            jedisPool.close();
            LOG.info("✅ Redis connection pool closed");
        }
        super.close();
    }
}