# Quick Start Guide

## Prerequisites
- Python 3.7 or higher
- Windows/Linux/MacOS

## Verification

Before running the system, verify everything is set up correctly:

```powershell
python test_system.py
```

This will check:
- Python version
- Required modules
- Required files
- Port availability

## Running the Complete System

### Option 1: Automated Start (Recommended)

Start all components automatically:

```powershell
python run_system.py
```

This will:
1. Start all service nodes (Q1, D, P11-P13, Q21-Q23, P21-P23)
2. Wait for initialization
3. Start client nodes (K1, K2)
4. Wait for both clients to finish, then shut everything down automatically (Ctrl+C still works to stop early)

### Option 2: Manual Start (For Testing/Debugging)

Open **13 separate terminal windows** and run each command:

#### Terminal 1-11: Service Nodes (Start in order)
```powershell
# Terminal 1: Q1 Distributor
python q1_node.py Q1

# Terminal 2: High-Performance Distributor
python d_node.py

# Terminal 3-5: P1x Service Nodes
python p1x_service.py P11
python p1x_service.py P12
python p1x_service.py P13

# Terminal 6-8: Q2x Queue Nodes
python q2x_node.py Q21
python q2x_node.py Q22
python q2x_node.py Q23

# Terminal 9-11: P2x Service Nodes (can fail)
python p2x_service.py P21
python p2x_service.py P22
python p2x_service.py P23
```

Wait for all services to start (about 3 seconds), then start clients:

#### Terminal 12-13: Clients
```powershell
# Terminal 12: Client K1
python client.py K1

# Terminal 13: Client K2
python client.py K2
```

## What to Expect

### During Execution

You'll see log messages showing:
- Requests being sent from clients
- Cyclic distribution at Q1
- Priority-based processing at P1x nodes
- Type-based routing at D
- P2x node failures and recoveries
- Responses being sent back to clients

### At Completion

Each client will print a summary:
```
==================================================
Client K1 Summary
==================================================
Total Requests Sent: 20
Successful Requests: 17
Failed Requests: 3
Success Rate: 85.0%
==================================================
```

## Configuration

Edit `config.py` to adjust:

### Network Settings
```python
HOST = 'localhost'
PORTS = {
    'K1': 5001,
    'K2': 5002,
    'Q1': 5010,
    # ... etc
}
```

### Request Priorities
```python
PRIORITIES = {
    'z1': 1,
    'z2': 2,
    'z3': 3,
}
```

### Service Times
```python
SERVICE_TIME = {
    'P11': {'min': 0.1, 'max': 0.3},
    'P12': {'min': 0.1, 'max': 0.3},
    # ... etc
}
```

### Client Behavior
```python
CLIENT_CONFIG = {
    'request_interval': 0.5,      # Time between requests
    'response_timeout': 10.0,     # Wait time for responses
    'num_requests': 20,           # Total requests per client
}
```

### Failure Simulation
```python
P2X_FAILURE_CONFIG = {
    'failure_probability': 0.1,   # 10% chance of failure
    'failure_duration': (3, 8),   # 3-8 seconds down time
    'check_interval': 2.0,        # Check every 2 seconds
}
```

### Dynamic Scaling
```python
SCALING_CONFIG = {
    'check_interval': 3.0,           # Monitor every 3 seconds
    'window_size': 10,               # Last 10 requests
    'scale_up_threshold': 1.5,       # 1.5x threshold triggers scaling
    'max_instances': 3,              # Max 3 instances per service
}
```

## Troubleshooting

### Port Already in Use
If you get "Address already in use" errors:

1. Check what's using the port:
```powershell
netstat -ano | findstr :5001
```

2. Kill the process or change ports in `config.py`

### Connection Refused
If clients can't connect to services:
- Ensure all services started successfully
- Wait 3 seconds after starting services before starting clients
- Check firewall settings

### Python Version Issues
Ensure you're using Python 3.7+:
```powershell
python --version
```

### Module Import Errors
The system uses only standard library modules. If you get import errors, check your Python installation.

## Stopping the System

### Automated Mode
Press `Ctrl+C` once. The orchestrator will gracefully shut down all components.

### Manual Mode
Press `Ctrl+C` in each terminal window.

## Testing Specific Scenarios

### Test Priority Queues
Modify `client.py` to send only high-priority requests:
```python
req_type = 'z3'  # All high priority
```

### Test Failure Tolerance
Increase failure rate in `config.py`:
```python
'failure_probability': 0.5,  # 50% failure rate
```

### Test Load Balancing
Enable debug logging to see cyclic distribution:
```python
LOG_LEVEL = 'DEBUG'
```

### Test Dynamic Scaling
Increase load to trigger scaling:
```python
CLIENT_CONFIG = {
    'request_interval': 0.1,  # Faster requests
    'num_requests': 100,      # More requests
}
```

## Expected Output

### Normal Operation
```
2025-12-03 14:30:01 - K1 - INFO - Sent z1 request K1_0_1733235001000
2025-12-03 14:30:01 - Q1 - INFO - Received z1 request K1_0_1733235001000
2025-12-03 14:30:01 - Q1 - INFO - Forwarded K1_0_1733235001000 to P11
2025-12-03 14:30:01 - P11 - INFO - Received z1 request K1_0_1733235001000
2025-12-03 14:30:01 - P11 - INFO - Processed K1_0_1733235001000
2025-12-03 14:30:01 - D - INFO - Received z1 request K1_0_1733235001000
...
```

### P2x Failure
```
2025-12-03 14:30:15 - P21 - WARNING - ⚠️  P21 GOING DOWN for 5.3s
2025-12-03 14:30:20 - P21 - INFO - ✓ P21 RECOVERED and back online
```

### Scaling Event
```
2025-12-03 14:30:25 - ScalingManager - WARNING - ⚡ SCALING UP P11 - Starting instance #2
```

## Architecture Quick Reference

```
Request Flow:
K1/K2 → Q1 → P11/P12/P13 → D → Q21/Q22/Q23 → P21/P22/P23 → K1/K2

Priority: z3 > z2 > z1
Q1 Strategy: Round-robin (cyclic)
D Strategy: Type-based routing
P2x: Can fail and recover
```

## Next Steps

1. Run `python test_system.py` to verify setup
2. Run `python run_system.py` to start the system
3. Observe the output and statistics
4. Experiment with different configurations
5. Review `FORMAL_SPECIFICATION.md` for mathematical details

## Support

For issues or questions:
1. Check logs for error messages
2. Verify configuration in `config.py`
3. Ensure all ports are available
4. Confirm Python 3.7+ is installed
