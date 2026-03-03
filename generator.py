import random
from datetime import datetime, timedelta
import csv
import os
from turtle import pd
from utils.config import THRESHOLDS

class Engine:
    def __init__(self, engine_id, is_malfunctioning=False):
        self.engine_id = engine_id
        self.is_malfunctioning = is_malfunctioning
        
        # Starting State
        self.timestamp = datetime.now()
        self.rpm = random.uniform(500, 7000)
        self.temp = random.uniform(20.0, 70.0)
        self.oil_pressure = 20 + (self.rpm * 0.01) + random.uniform(0.5, 5.5)
        self.fuel_cons = (self.rpm * 0.0001) + random.uniform(0.0, 0.5)
        self.status = "idle"
        
        self.minutes_running = 0
        
    def update(self,target_rpm):
        """Simulates the engine running for one minute, updating telemetry."""
        self.timestamp += timedelta(minutes=1)
        self.minutes_running += 1

        # Simulate RPM changes
        if self.rpm < 0:
            self.rpm = target_rpm
        if 0 < target_rpm <= THRESHOLDS['rpm']['max']:
            self.rpm =  target_rpm + random.uniform(0.0, 100.0)
        else:
            self.rpm = 0.0
            
        # Simulate temperature changes
        if self.temp > THRESHOLDS['temp']['max']:
             self.temp = 85.0
        elif self.rpm > 3000:
            self.temp += random.uniform(0.5, 2.0) 
        elif self.rpm > 0:
            self.temp += (90 - self.temp) * 0.1 + random.uniform(0.1, 0.5) 
        else:
            self.temp -= (self.temp - 20) * 0.1

        # Simulate oil pressure changes
        self.oil_pressure = self.rpm * 0.012 + random.uniform(2.0, 5.0)

        # Simulate fuel consumption changes
        self.fuel_cons = self.rpm * 0.005 + random.uniform(0.1, 0.8)

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
        
        if self.rpm > THRESHOLDS['rpm']['max']:
            return "error: overrevving"
        
        if self.rpm < THRESHOLDS['rpm']['min']:
            return "error: rpm_sensor_fault"
        
        if self.temp > THRESHOLDS['temp']['max']:
            return "error: overheating"

        if self.oil_pressure is None:
            return "error: oil_sensor_fault"  
        
        if self.oil_pressure < THRESHOLDS['oil_pressure']['min'] and self.rpm > 500:
            return "error: low_oil_pressure"
            
        if self.status == 'running' and self.rpm == 0 and self.temp > 80:
            return "error: stalled"
        
        if self.fuel_cons > THRESHOLDS['fuel_consumption']['max']:
            return "error: high_fuel_consumption"
        
        if self.fuel_cons < THRESHOLDS['fuel_consumption']['min']:
            return "error: fuel_sensor_fault"
        
        if self.fuel_cons < THRESHOLDS['fuel_consumption']['min'] and self.rpm > 1000:        
            return "error: fuel_leak"

        # Warnings
        if self.rpm > THRESHOLDS['rpm']['max'] * 0.9:
            return "warning: high_rpm"
            
        if self.temp > THRESHOLDS['temp']['max'] * 0.9:
            return "warning: high_temp"
            
        if self.oil_pressure < THRESHOLDS['oil_pressure']['min'] and self.rpm > 500:
            return "warning: low_oil_pressure"

        if self.fuel_cons > THRESHOLDS['fuel_consumption']['max'] * 0.9:
            return "warning: high_fuel_consumption"
        
        if self.fuel_cons < THRESHOLDS['fuel_consumption']['min'] * 1.1 and self.rpm > 1000:
            return "warning: low_fuel_consumption"

        # Normal
        if self.rpm > THRESHOLDS['rpm']['min'] and self.rpm <= THRESHOLDS['rpm']['max'] and self.temp <= THRESHOLDS['temp']['max']:       
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
            "fuel_consumption": self.fuel_cons,
            "status": self.status,
            "is_malfunctioning": self.is_malfunctioning
        }
    
output_dir = "engine_data"
os.makedirs(output_dir, exist_ok=True)

engines = [
    Engine(engine_id="ENG-001", is_malfunctioning=False),
    Engine(engine_id="ENG-002", is_malfunctioning=True)
]

# Define a simple 90-minute drive cycle (target RPMs)
drive_cycle = [800]*10 + [3500]*40 + [1200]*10 + [3500]*10 + [1200]*20

# Generation Loop
for engine in engines:
    file_path = os.path.join(output_dir, f"telemetry_{engine.engine_id}.csv")
    
    with open(file_path, mode='w', newline='') as f:

        first_reading = engine.get_telemetry()
        writer = csv.DictWriter(f, fieldnames=first_reading.keys())
        writer.writeheader()
        writer.writerow(first_reading)

        for minute in range(90):
            target = drive_cycle[minute]
            engine.update(target)
            telemetry_data = engine.get_telemetry()
            writer.writerow(telemetry_data)
            # Chance to inject a duplicate
            if random.random() < 0.05:
                writer.writerow(telemetry_data)