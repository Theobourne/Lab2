"""
Main orchestrator script to start all system components
"""

import subprocess
import time
import sys
import signal
import logging
from typing import List

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger("Orchestrator")

# All nodes in the system
NODES = [
    # Queue and distribution nodes
    ('Q1', 'q1_node.py'),
    ('D', 'd_node.py'),
    
    # P1x service nodes
    ('P11', 'p1x_service.py'),
    ('P12', 'p1x_service.py'),
    ('P13', 'p1x_service.py'),
    
    # Q2x queue nodes
    ('Q21', 'q2x_node.py'),
    ('Q22', 'q2x_node.py'),
    ('Q23', 'q2x_node.py'),
    
    # P2x service nodes (can fail)
    ('P21', 'p2x_service.py'),
    ('P22', 'p2x_service.py'),
    ('P23', 'p2x_service.py'),
]

CLIENT_NODES = [
    ('K1', 'client.py'),
    ('K2', 'client.py'),
]

def start_node(node_name: str, script: str) -> subprocess.Popen:
    """Start a single node"""
    logger.info(f"Starting {node_name}...")
    
    # Start the process - removed output redirection to see logs
    proc = subprocess.Popen(
        [sys.executable, script, node_name],
        text=True
    )
    
    return proc


def start_group(group) -> List[tuple]:
    """Start a group of nodes and return list of (name, process)."""
    procs: List[tuple] = []
    for node_name, script in group:
        proc = start_node(node_name, script)
        procs.append((node_name, proc))
        time.sleep(0.5)  # Small delay between starts
    return procs


def stop_processes(processes: List[tuple]):
    """Terminate a list of (name, process) pairs."""
    for name, proc in processes:
        try:
            proc.terminate()
        except Exception:
            logger.debug("Process %s already stopped", name)
    
    time.sleep(2)
    
    for name, proc in processes:
        try:
            proc.kill()
        except Exception:
            pass


def wait_for_clients(client_procs: List[tuple], service_procs: List[tuple]) -> None:
    """Block until all clients exit; if a service dies early, bail out."""
    logger.info("Waiting for clients to finish; the system will stop automatically...")
    
    remaining = dict(client_procs)
    while remaining:
        time.sleep(1)
        
        # If any critical service died, stop everything
        for svc_name, svc_proc in service_procs:
            if svc_proc.poll() is not None:
                logger.error("Critical node %s exited early (code %s). Stopping system.",
                             svc_name, svc_proc.returncode)
                return
        
        # Track finished clients
        finished = []
        for name, proc in remaining.items():
            ret = proc.poll()
            if ret is not None:
                finished.append(name)
                logger.info("Client %s finished with exit code %s", name, ret)
        
        for name in finished:
            remaining.pop(name, None)
    
    logger.info("All clients finished. Shutting down services.")


def signal_handler(sig, frame):
    """Handle Ctrl+C"""
    raise KeyboardInterrupt


def main():
    # Register signal handler
    signal.signal(signal.SIGINT, signal_handler)
    
    try:
        logger.info("="*60)
        logger.info("STARTING DISTRIBUTED SYSTEM")
        logger.info("="*60)
        
        # Start services
        service_procs = start_group(NODES)
        logger.info("\nAll system nodes started. Waiting for initialization...")
        time.sleep(3)
        
        # Start clients
        logger.info("\n" + "="*60)
        logger.info("STARTING CLIENTS")
        logger.info("="*60)
        client_procs = start_group(CLIENT_NODES)
        logger.info("\nAll clients started. System is now running.")
        
        # Wait for clients to finish; then shut everything down
        wait_for_clients(client_procs, service_procs)
        
    except KeyboardInterrupt:
        logger.info("Shutdown requested by user.")
    finally:
        logger.info("\n" + "="*60)
        logger.info("SHUTTING DOWN DISTRIBUTED SYSTEM")
        logger.info("="*60)
        # Stop clients first (if any still running), then services
        stop_processes(client_procs if 'client_procs' in locals() else [])
        stop_processes(service_procs if 'service_procs' in locals() else [])
        logger.info("All nodes stopped.")


if __name__ == '__main__':
    main()
