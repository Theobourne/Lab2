"""
Configuration file for the Distributed System Simulation
"""

# Network Configuration
HOST = 'localhost'

# Port assignments for all nodes
PORTS = {
    # Clients
    'K1': 5001,
    'K2': 5002,
    
    # Queue Q1 (Cyclic distributor)
    'Q1': 5010,
    
    # Service devices P1x
    'P11': 5011,
    'P12': 5012,
    'P13': 5013,
    
    # High-performance distributor D
    'D': 5020,
    
    # Queues Q2x
    'Q21': 5021,
    'Q22': 5022,
    'Q23': 5023,
    
    # Service devices P2x (can fail)
    'P21': 5031,
    'P22': 5032,
    'P23': 5033,
}

# Request priorities (higher number = higher priority)
PRIORITIES = {
    'z1': 1,
    'z2': 2,
    'z3': 3,
}

# Service time configurations (in seconds)
SERVICE_TIME = {
    'P11': {'min': 0.1, 'max': 0.3},
    'P12': {'min': 0.1, 'max': 0.3},
    'P13': {'min': 0.1, 'max': 0.3},
    'P21': {'min': 0.2, 'max': 0.5},
    'P22': {'min': 0.2, 'max': 0.5},
    'P23': {'min': 0.2, 'max': 0.5},
}

# Queue configuration
QUEUE_CONFIG = {
    'max_length': 50,
    'max_wait_time': 5.0,  # seconds
    'avg_wait_time_threshold': 2.0,  # seconds - triggers scaling
}

# Client configuration
CLIENT_CONFIG = {
    'request_interval': 0.5,  # seconds between requests
    'response_timeout': 12.0,  # seconds to wait for all responses (more realistic tolerance)
    'num_requests': 100,  # number of requests to send per client
}

# P2x failure simulation
P2X_FAILURE_CONFIG = {
    'failure_probability': 0.05,  # 5% chance of being down (more realistic)
    'failure_duration': (3, 8),  # seconds (min, max)
    'check_interval': 2.0,  # seconds between failure checks
}

# Dynamic scaling configuration
SCALING_CONFIG = {
    'check_interval': 3.0,  # seconds between scaling checks
    'window_size': 10,  # number of recent requests to consider
    'scale_up_threshold': 1.5,  # average wait time multiplier
    'max_instances': 3,  # maximum instances per service type
}

# Logging
LOG_LEVEL = 'INFO'  # DEBUG, INFO, WARNING, ERROR
LOG_FILE = 'system.log'
