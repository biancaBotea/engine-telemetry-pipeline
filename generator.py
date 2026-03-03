import random
from datetime import datetime, timedelta

class Engine:
    def __init__(self, engine_id, is_malfunctioning=False):
        self.engine_id = engine_id
        self.is_malfunctioning = is_malfunctioning
        
        # Starting State
        self.timestamp = datetime.now()
        self.rpm = 0.0
        self.max_rpm = 5000.0
        self.temp = random.uniform(20.0, 40.0)
        self.oil_pressure = 0.0
        self.fuel_cons = 0.0
        self.status = "idle"
        
        self.minutes_running = 0
        
    def update(self,target_rpm):
        """Simulates the engine running for one minute, updating telemetry."""
        self.timestamp += timedelta(minutes=1)
        self.minutes_running += 1

        # Simulate RPM changes
        if self.rpm < 0:
            self.rpm = target_rpm
        if 0 < target_rpm <= self.max_rpm:
            self.rpm =  target_rpm + random.uniform(0.0, 100.0)
        else:
            self.rpm = 0.0
            
        # Simulate temperature changes
        if self.temp > 150:
             self.temp = 85.0
        elif self.rpm > 0:
            self.temp += (90 - self.temp) * 0.1 + random.uniform(0.1, 0.5) 
        else:
            self.temp -= (self.temp - 20) * 0.1 + random.uniform(0.1, 0.5)

        # Simulate oil pressure changes
        self.oil_pressure = self.rpm * 0.01 + random.uniform(0.0, 5.0)

        # Simulate fuel consumption        
        self.fuel_cons = self.rpm * 0.0001

        # Unusual Readings
        chaos_chance = 0.1 if self.is_malfunctioning else 0.01
        
        if random.random() < chaos_chance:
            anomaly_type = random.choice(['null', 'temperature_spike', 'out_of_range'])
            
            if anomaly_type == 'null':
                self.oil_pressure = None
            elif anomaly_type == 'temperature_spike':
                self.temp = 999.9
            elif anomaly_type == 'out_of_range':
                self.rpm = -999.9


        self.status = self._check_status()
        pass

    def _check_status(self):
        """Internal method to evaluate engine health based on current sensors."""
        
        # Errors
        if self.is_malfunctioning and random.random() < 0.01:
             return "error: general malfunction"
        
        if self.rpm > self.max_rpm:
            return "error: overrevving"
        
        if self.temp > 125:
            return "error: overheating"

        if self.oil_pressure is None:
            return "error: oil_sensor_fault"  
        
        if self.oil_pressure < 0.5 and self.rpm > 500: # Only error if engine is actually trying to run
            return "error: low oil pressure"
            
        if self.status == 'running' and self.rpm == 0 and self.temp > 80:
            return "error: stalled"
        
        if self.rpm < 0:
            return "error: rpm_sensor_fault"

        # Warnings
        if self.rpm > self.max_rpm * 0.9:
            return "warning: high_rpm"
            
        if self.temp > 105:
            return "warning: high_temp"
            
        if self.oil_pressure < 1.5 and self.rpm > 500:
            return "warning: low_oil_pressure"

        # Normal
        if self.rpm > 100:       
            return "running" 
        
        return "idle"

    def get_telemetry(self):
        """Returns the current state as a dictionary for the CSV."""
        return {
            "timestamp": self.timestamp.isoformat(),
            "engine_id": self.engine_id,
            "rpm": self.rpm,
            "temp": self.temp,
            "oil_pressure": self.oil_pressure,
            "fule_cons": self.fuel_cons,
            "status": self.status,
            "is_malfunctioning": self.is_malfunctioning
        }
    
import csv
import os

# 1. Setup the environment
output_dir = "engine_data"
os.makedirs(output_dir, exist_ok=True)

# 2. Instantiate your engines
engines = [
    Engine(engine_id="ENG-001", is_malfunctioning=False), # Healthy
    Engine(engine_id="ENG-002", is_malfunctioning=True)   # The "Chaos" engine
]

# 3. Define a simple 60-minute drive cycle (target RPMs)
# 10 mins idle, 40 mins cruising, 10 mins slowing down
drive_cycle = [800]*10 + [3500]*40 + [1200]*10 + [3500]*10 + [1200]*20

# 4. The Generation Loop
for engine in engines:
    file_path = os.path.join(output_dir, f"telemetry_{engine.engine_id}.csv")
    
    with open(file_path, mode='w', newline='') as f:
        # Get the keys from the first telemetry reading to use as headers
        first_reading = engine.get_telemetry()
        writer = csv.DictWriter(f, fieldnames=first_reading.keys())
        writer.writeheader()
        writer.writerow(first_reading) # Write the t=0 state

        # Run the 60 minute simulation
        for minute in range(90):
            target = drive_cycle[minute]
            engine.update(target)
            writer.writerow(engine.get_telemetry())