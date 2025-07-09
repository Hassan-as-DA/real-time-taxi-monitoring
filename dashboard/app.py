from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from fastapi.middleware.cors import CORSMiddleware
import redis.asyncio as redis
import json
import asyncio
import logging
from typing import Dict, List, Set, Optional
import time
from contextlib import asynccontextmanager
import os
from dataclasses import dataclass, asdict
from datetime import datetime

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@dataclass
class TaxiData:
    taxi_id: str
    latitude: float
    longitude: float
    speed: float
    avg_speed: float
    total_distance: float
    timestamp: str
    last_updated: int

@dataclass
class FleetStats:
    active_taxis_count: int
    fleet_total_distance: float
    recent_alerts_count: int
    last_updated: int

class ConnectionManager:
    def __init__(self):
        self.active_connections: List[WebSocket] = []
        self.last_broadcast: Dict[str, float] = {}
        
    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)
        logger.info(f"Client connected. Total connections: {len(self.active_connections)}")
        
    def disconnect(self, websocket: WebSocket):
        if websocket in self.active_connections:
            self.active_connections.remove(websocket)
        logger.info(f"Client disconnected. Total connections: {len(self.active_connections)}")
        
    async def broadcast(self, message_type: str, data: dict):
        if not self.active_connections:
            return
            
        message = {"type": message_type, "data": data, "timestamp": int(time.time())}
        message_json = json.dumps(message)
        
        # Remove disconnected clients
        disconnected = []
        for connection in self.active_connections:
            try:
                await connection.send_text(message_json)
            except:
                disconnected.append(connection)
                
        for conn in disconnected:
            self.disconnect(conn)
            
        logger.debug(f"Broadcasted {message_type} to {len(self.active_connections)} clients")

class RedisOptimized:
    def __init__(self):
        self.redis_client: Optional[redis.Redis] = None
        self.connection_pool = None
        
    async def connect(self):
        try:
            # Optimized connection pool
            self.connection_pool = redis.ConnectionPool(
                host=os.getenv('REDIS_HOST', 'redis'),
                port=int(os.getenv('REDIS_PORT', 6379)),
                decode_responses=True,
                max_connections=20,
                retry_on_timeout=True,
                socket_connect_timeout=5,
                socket_timeout=10,
                health_check_interval=30
            )
            
            self.redis_client = redis.Redis(connection_pool=self.connection_pool)
            await self.redis_client.ping()
            logger.info("✅ Redis connection established with optimized pool")
            
        except Exception as e:
            logger.error(f"❌ Redis connection failed: {e}")
            raise
            
    async def close(self):
        if self.redis_client:
            await self.redis_client.close()
        if self.connection_pool:
            await self.connection_pool.disconnect()
            
    async def get_all_taxi_data_optimized(self) -> Dict[str, TaxiData]:
        """Ultra-fast batch retrieval of all taxi data using pipeline"""
        try:
            # Get active taxis first
            active_taxis = await self.redis_client.smembers('active_taxis')
            if not active_taxis:
                return {}
                
            # Use pipeline for batch operations - MASSIVE performance boost
            pipe = self.redis_client.pipeline()
            
            # Queue all operations at once
            for taxi_id in active_taxis:
                pipe.hgetall(f"taxi:{taxi_id}")
                
            # Execute all operations in one round trip
            results = await pipe.execute()
            
            # Process results
            taxi_data = {}
            for i, taxi_id in enumerate(active_taxis):
                data = results[i]
                if data and 'latitude' in data and 'longitude' in data:
                    try:
                        taxi_data[taxi_id] = TaxiData(
                            taxi_id=taxi_id,
                            latitude=float(data['latitude']),
                            longitude=float(data['longitude']),
                            speed=float(data.get('speed', 0)),
                            avg_speed=float(data.get('avg_speed', 0)),
                            total_distance=float(data.get('total_distance', 0)),
                            timestamp=data.get('timestamp', ''),
                            last_updated=int(time.time())
                        )
                    except (ValueError, KeyError) as e:
                        logger.warning(f"Invalid taxi data for {taxi_id}: {e}")
                        
            logger.info(f"⚡ Retrieved {len(taxi_data)} taxis in single batch operation")
            return taxi_data
            
        except Exception as e:
            logger.error(f"Error getting taxi data: {e}")
            return {}
    
    async def get_fleet_stats_optimized(self) -> FleetStats:
        """Optimized fleet stats retrieval"""
        try:
            pipe = self.redis_client.pipeline()
            pipe.scard('active_taxis')
            pipe.get('fleet_total_distance')
            pipe.llen('alerts')
            
            results = await pipe.execute()
            
            return FleetStats(
                active_taxis_count=results[0] or 0,
                fleet_total_distance=float(results[1] or 0),
                recent_alerts_count=results[2] or 0,
                last_updated=int(time.time())
            )
        except Exception as e:
            logger.error(f"Error getting fleet stats: {e}")
            return FleetStats(0, 0.0, 0, int(time.time()))
    
    async def get_recent_alerts_optimized(self) -> List[Dict]:
        """Optimized alert retrieval with parsing"""
        try:
            # Get recent alerts
            alerts = await self.redis_client.lrange('alerts', 0, 19)  # Last 20 alerts
            
            parsed_alerts = []
            for alert in alerts:
                try:
                    # Try parsing as JSON first (new format)
                    if alert.startswith('{'):
                        alert_data = json.loads(alert)
                        parsed_alerts.append(alert_data)
                    else:
                        # Parse old string format
                        if 'Speeding' in alert:
                            taxi_match = alert.split('Taxi ')[1].split(' ')[0] if 'Taxi ' in alert else 'Unknown'
                            time_match = alert.split(' at ')[-1] if ' at ' in alert else 'Unknown'
                            parsed_alerts.append({
                                'type': 'speeding',
                                'taxi_id': taxi_match,
                                'timestamp': time_match,
                                'message': alert
                            })
                        elif 'Area Violation' in alert:
                            taxi_match = alert.split('Taxi ')[1].split(' ')[0] if 'Taxi ' in alert else 'Unknown'
                            time_match = alert.split(' at ')[-1] if ' at ' in alert else 'Unknown'
                            parsed_alerts.append({
                                'type': 'area_violation',
                                'taxi_id': taxi_match,
                                'timestamp': time_match,
                                'message': alert
                            })
                except Exception as e:
                    logger.warning(f"Failed to parse alert: {alert}, error: {e}")
                    
            return parsed_alerts
            
        except Exception as e:
            logger.error(f"Error getting alerts: {e}")
            return []

# Global instances
redis_client = RedisOptimized()
connection_manager = ConnectionManager()

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    await redis_client.connect()
    
    # Start background tasks
    asyncio.create_task(taxi_data_broadcaster())
    asyncio.create_task(fleet_stats_broadcaster())
    asyncio.create_task(alerts_broadcaster())
    
    yield
    
    # Shutdown
    await redis_client.close()

app = FastAPI(title="Real-Time Taxi Monitoring API", lifespan=lifespan)

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Serve static files
app.mount("/static", StaticFiles(directory="static"), name="static")

@app.get("/")
async def serve_dashboard():
    return FileResponse("index.html")

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await connection_manager.connect(websocket)
    try:
        # Send initial data immediately
        taxi_data = await redis_client.get_all_taxi_data_optimized()
        fleet_stats = await redis_client.get_fleet_stats_optimized()
        alerts = await redis_client.get_recent_alerts_optimized()
        
        await websocket.send_text(json.dumps({
            "type": "initial_data",
            "data": {
                "taxis": {k: asdict(v) for k, v in taxi_data.items()},
                "stats": asdict(fleet_stats),
                "alerts": alerts
            }
        }))
        
        # Keep connection alive
        while True:
            await websocket.receive_text()
            
    except WebSocketDisconnect:
        connection_manager.disconnect(websocket)
    except Exception as e:
        logger.error(f"WebSocket error: {e}")
        connection_manager.disconnect(websocket)

# Background broadcasters
async def taxi_data_broadcaster():
    """Broadcast taxi locations every 2 seconds"""
    while True:
        try:
            taxi_data = await redis_client.get_all_taxi_data_optimized()
            if taxi_data:
                await connection_manager.broadcast("taxi_update", {
                    "taxis": {k: asdict(v) for k, v in taxi_data.items()}
                })
            await asyncio.sleep(2)
        except Exception as e:
            logger.error(f"Error in taxi broadcaster: {e}")
            await asyncio.sleep(5)

async def fleet_stats_broadcaster():
    """Broadcast fleet stats every 5 seconds"""
    while True:
        try:
            stats = await redis_client.get_fleet_stats_optimized()
            await connection_manager.broadcast("stats_update", asdict(stats))
            await asyncio.sleep(5)
        except Exception as e:
            logger.error(f"Error in stats broadcaster: {e}")
            await asyncio.sleep(5)

async def alerts_broadcaster():
    """Broadcast alerts every 1 second for real-time alerts"""
    while True:
        try:
            alerts = await redis_client.get_recent_alerts_optimized()
            await connection_manager.broadcast("alerts_update", {"alerts": alerts})
            await asyncio.sleep(1)
        except Exception as e:
            logger.error(f"Error in alerts broadcaster: {e}")
            await asyncio.sleep(3)

# REST API endpoints (for backward compatibility and health checks)
@app.get("/api/health")
async def health_check():
    try:
        await redis_client.redis_client.ping()
        stats = await redis_client.get_fleet_stats_optimized()
        return {
            "status": "healthy",
            "redis_connected": True,
            "active_taxis": stats.active_taxis_count,
            "timestamp": int(time.time())
        }
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"Service unhealthy: {str(e)}")

@app.get("/api/taxi_data")
async def get_all_taxi_data():
    """Get all taxi data in one call"""
    taxi_data = await redis_client.get_all_taxi_data_optimized()
    return {k: asdict(v) for k, v in taxi_data.items()}

@app.get("/api/stats")
async def get_fleet_stats():
    """Get fleet statistics"""
    stats = await redis_client.get_fleet_stats_optimized()
    return asdict(stats)

@app.get("/api/alerts")
async def get_alerts():
    """Get recent alerts"""
    alerts = await redis_client.get_recent_alerts_optimized()
    return {"alerts": alerts}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "app:app",
        host="0.0.0.0",
        port=int(os.getenv("PORT", 8000)),
        reload=False,
        access_log=True,
        loop="asyncio"
    )