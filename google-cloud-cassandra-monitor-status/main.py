#!/usr/bin/env python3
# pylint: disable=too-many-locals, too-many-arguments, broad-except, line-too-long

"""
Cassandra Node Ring Monitoring Script

This script runs on a central utility server, connects to specified Cassandra GCE VMs
via IAP, executes 'nodetool ring', parses the output, and sends metrics to
Google Cloud Monitoring. It loads configuration from a 'config.yaml' file.
"""

import subprocess
import re
import time
import json # Not strictly used in this version, but often useful
import logging
import yaml # For loading YAML config
import os
from typing import List, Dict, Any, Optional, Tuple

from google.cloud import monitoring_v3
from google.protobuf.timestamp_pb2 import Timestamp # For setting custom timestamps

# --- Configuration Loading ---
CONFIG_FILE_PATH = "/opt/techops/Alerts/cassandra_monitor/config.yaml" # Or provide an absolute path

def load_config(config_path: str) -> Dict[str, Any]:
    """Loads configuration from a YAML file."""
    if not os.path.exists(config_path):
        logging.critical(f"Configuration file not found: {config_path}")
        raise FileNotFoundError(f"Configuration file not found: {config_path}")
    with open(config_path, 'r', encoding='utf-8') as f:
        config = yaml.safe_load(f)
    
    # Basic validation
    required_keys = ["project_id", "common_cassandra_settings", "timeouts", "cassandra_instances"]
    for key in required_keys:
        if key not in config:
            logging.critical(f"Missing required key '{key}' in config file: {config_path}")
            raise ValueError(f"Missing required key '{key}' in config file: {config_path}")
    
    common_settings_keys = ["ssh_user", "nodetool_path"]
    for key in common_settings_keys:
        if key not in config["common_cassandra_settings"]:
            logging.critical(f"Missing required key '{key}' in common_cassandra_settings in config file: {config_path}")
            raise ValueError(f"Missing required key '{key}' in common_cassandra_settings in config file: {config_path}")
        # Add check to ensure nodetool_path is not a directory ending with /
        if key == "nodetool_path" and config["common_cassandra_settings"][key].endswith('/'):
            logging.critical(f"Configuration error: 'nodetool_path' in common_cassandra_settings should be the full path to the executable, not a directory (e.g., /path/to/nodetool, not /path/to/nodetool/). Current value: {config['common_cassandra_settings'][key]}")
            raise ValueError(f"Configuration error: 'nodetool_path' in common_cassandra_settings should be the full path to the executable. Current value: {config['common_cassandra_settings'][key]}")


    timeout_keys = ["ssh_connect_timeout_sec", "command_timeout_sec"]
    for key in timeout_keys:
        if key not in config["timeouts"]:
            logging.critical(f"Missing required key '{key}' in timeouts in config file: {config_path}")
            raise ValueError(f"Missing required key '{key}' in timeouts in config file: {config_path}")

    if not isinstance(config["cassandra_instances"], list) or not config["cassandra_instances"]:
        logging.critical("'cassandra_instances' must be a non-empty list in config file.")
        raise ValueError("'cassandra_instances' must be a non-empty list in config file.")

    instance_keys = ["instance_name", "instance_id", "ip", "zone"]
    for i, instance_conf in enumerate(config["cassandra_instances"]):
        for key in instance_keys:
            if key not in instance_conf:
                 logging.critical(f"Missing required key '{key}' in cassandra_instances[{i}] in config file.")
                 raise ValueError(f"Missing required key '{key}' in cassandra_instances[{i}] in config file.")
    return config

# --- Logging Configuration (Setup early) ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(filename)s:%(lineno)d - %(message)s",
    handlers=[logging.StreamHandler()]
)

# Load configuration immediately
try:
    CONFIG = load_config(CONFIG_FILE_PATH)
except (FileNotFoundError, ValueError) as e:
    logging.critical(f"Failed to load or validate configuration: {e}")
    exit(1) # Exit if config is bad

PROJECT_ID = CONFIG["project_id"]
COMMON_SETTINGS = CONFIG["common_cassandra_settings"]
TIMEOUTS = CONFIG["timeouts"]

# Construct CASSANDRA_NODES by merging common settings with instance-specific details
CASSANDRA_NODES = []
for instance_conf in CONFIG["cassandra_instances"]:
    node_detail = {**instance_conf, **COMMON_SETTINGS} # Merge dicts
    CASSANDRA_NODES.append(node_detail)


# --- GCP Cloud Monitoring Client ---
METRIC_CLIENT = monitoring_v3.MetricServiceClient()
PROJECT_NAME = f"projects/{PROJECT_ID}"


# --- Metric Definitions ---
METRIC_REPORTER_REACHABILITY = "custom.googleapis.com/cassandra/reporter_node/reachability_status"
METRIC_NODE_HEALTH = "custom.googleapis.com/cassandra/node/health_status"
METRIC_NODE_OWNERSHIP = "custom.googleapis.com/cassandra/node/effective_ownership"
METRIC_NODE_LOAD_BYTES = "custom.googleapis.com/cassandra/node/load_bytes"
METRIC_NODE_LOAD_PERCENTAGE = "custom.googleapis.com/cassandra/node/load_percentage_of_total"

# --- Helper Functions ---

def get_current_timestamp_pb() -> Timestamp:
    """Returns the current time as a Google Protobuf Timestamp."""
    now = time.time()
    seconds = int(now)
    nanos = int((now - seconds) * 10**9)
    return Timestamp(seconds=seconds, nanos=nanos)

def ping_check(target_ip: str, count: int = 3, timeout_sec: int = 2) -> bool:
    """
    Performs a simple ping check to the target IP.
    Returns True if successful, False otherwise.
    """
    logging.info(f"Pinging {target_ip}...")
    try:
        command = ["ping", "-c", str(count), "-W", str(timeout_sec), target_ip]
        result = subprocess.run(command, capture_output=True, text=True, timeout=count * timeout_sec + 5, check=False)
        if result.returncode == 0:
            logging.info(f"Ping to {target_ip} successful.")
            return True
        logging.warning(f"Ping to {target_ip} failed. Return code: {result.returncode}\nStdout: {result.stdout}\nStderr: {result.stderr}")
        return False
    except subprocess.TimeoutExpired:
        logging.error(f"Ping command to {target_ip} timed out.")
        return False
    except Exception as e:
        logging.error(f"Error during ping to {target_ip}: {e}")
        return False

def execute_command_via_iap(
    project_id: str,
    zone: str,
    instance_name: str,
    ssh_user: str,
    command_to_execute: str, 
    ssh_connect_timeout: int, 
    command_timeout: int 
) -> Tuple[Optional[str], Optional[str]]:
    """
    Executes a command on a remote GCE instance via IAP tunnel using gcloud.
    The ssh_user is expected to have direct permission to run command_to_execute.
    Returns (stdout, error_message_string) or (None, error_message_string) on failure.
    """
    gcloud_command = [
        "gcloud", "compute", "ssh",
        f"{ssh_user}@{instance_name}", 
        f"--project={project_id}",
        f"--zone={zone}",
        "--tunnel-through-iap",
        f"--command={command_to_execute}", 
        "--ssh-flag=-o IdentitiesOnly=yes",
        "--ssh-flag=-o StrictHostKeyChecking=no",
        "--ssh-flag=-o UserKnownHostsFile=/dev/null",
        f"--ssh-flag=-o ConnectTimeout={ssh_connect_timeout}"
    ]
    logging.info(f"Executing on {instance_name}: {' '.join(gcloud_command)}")
    try:
        process = subprocess.run(
            gcloud_command,
            capture_output=True,
            text=True,
            timeout=command_timeout, 
            check=False 
        )
        if process.returncode == 0:
            logging.info(f"Command successful on {instance_name}. Output length: {len(process.stdout)}")
            return process.stdout, None
        
        error_message = (
            f"Command failed on {instance_name}. Return Code: {process.returncode}. "
            f"Stderr: {process.stderr.strip()}. Stdout: {process.stdout.strip()}"
        )
        logging.error(error_message)
        return None, error_message

    except subprocess.TimeoutExpired:
        error_message = f"gcloud ssh command to {instance_name} timed out after {command_timeout} seconds."
        logging.error(error_message)
        return None, error_message
    except Exception as e:
        error_message = f"Exception executing command on {instance_name}: {e}"
        logging.error(error_message)
        return None, error_message

def parse_nodetool_ring_output(ring_output: str, reporter_node_ip: str, reporter_instance_id: str, reporter_zone: str) -> List[Dict[str, Any]]:
    """
    Parses the output of 'nodetool ring'.
    Returns a list of dictionaries, each representing an observed node.
    """
    parsed_nodes = []
    lines = ring_output.strip().splitlines()

    # Regex to capture the main data fields from a node line
    # Adjust this regex based on the exact consistent spacing or column characteristics of your nodetool version.
    node_line_pattern = re.compile(
        r"^(?P<address>\S+)\s+"                                       # Node IP Address
        r"(?P<dc>\S+)\s+"                                             # Datacenter
        r"(?P<rack>\S+)\s+"                                           # Rack
        r"(?P<status>Up|Down)\s+"                                     # Status (Up or Down)
        r"(?P<state>\S+)\s+"                                          # State (Normal, Leaving, etc.)
        r"(?P<load_val>\d+\.\d+|\d+)\s+(?P<load_unit>GB|MB|TB|KB)\s+"  # Load value and unit
        r"(?P<ownership>\d+\.\d+)%\s+"                                # Effective Ownership percentage
        r"(?P<token>\S+)$"                                            # Token
    )

    header_skipped = False
    for line in lines:
        line_stripped = line.strip()
        if not line_stripped: # Skip empty lines
            continue

        # Skip header lines until we find what looks like the start of data
        if not header_skipped:
            if "Address" in line_stripped and "DC" in line_stripped and "Rack" in line_stripped:
                header_skipped = True
            continue # Skip the current line (either header or line before actual data)

        # Attempt to parse as a node data line
        match = node_line_pattern.match(line_stripped)
        if match:
            data = match.groupdict()
            
            load_bytes = 0
            try:
                val = float(data['load_val'])
                unit = data['load_unit'].upper()
                if unit == "GB": load_bytes = int(val * (1024**3))
                elif unit == "MB": load_bytes = int(val * (1024**2))
                elif unit == "TB": load_bytes = int(val * (1024**4))
                elif unit == "KB": load_bytes = int(val * 1024)
            except ValueError:
                logging.warning(f"Could not parse load value '{data['load_val']}' for node {data['address']}. Defaulting to 0.")

            parsed_nodes.append({
                "reporter_node_ip": reporter_node_ip,
                "reporter_instance_id": reporter_instance_id,
                "reporter_zone": reporter_zone,
                "observed_node_address": data["address"],
                "observed_node_datacenter": data["dc"],
                "observed_node_rack": data["rack"],
                "observed_node_actual_status": data["status"],
                "observed_node_actual_state": data["state"],
                "load_bytes": load_bytes,
                "effective_ownership_percentage": float(data["ownership"]),
                "health_value": 1 if data["status"].lower() == "up" and data["state"].lower() == "normal" else 0
            })
        elif line_stripped and not re.match(r"^\s*$", line_stripped) and "Note:" not in line_stripped:
            # This condition tries to catch lines that are not empty, not the header,
            # not a note, and didn't match the node pattern (like the long token line).
            logging.debug(f"Skipping non-data line or unparsable line: '{line_stripped}'")


    # Calculate load percentage of total for this reporter's view
    total_load_bytes_for_this_ring = sum(node.get("load_bytes", 0) for node in parsed_nodes)
    for node in parsed_nodes:
        if total_load_bytes_for_this_ring > 0:
            node["load_percentage_of_total"] = (node.get("load_bytes", 0) / total_load_bytes_for_this_ring) * 100.0
        else:
            node["load_percentage_of_total"] = 0.0 # Avoid division by zero if all loads are 0
            
    return parsed_nodes


def create_time_series(
    metric_type: str,
    metric_labels: Dict[str, str],
    resource_labels: Dict[str, str], # For GCE instance: instance_id, zone
    value: Any,
    value_type: str = "INT64" # or "DOUBLE"
) -> monitoring_v3.types.TimeSeries:
    """Helper to create a TimeSeries object."""
    series = monitoring_v3.types.TimeSeries()
    series.metric.type = metric_type
    for k, v_label in metric_labels.items(): # Renamed v to v_label to avoid conflict
        series.metric.labels[k] = str(v_label) # Ensure labels are strings

    series.resource.type = "gce_instance" # Monitored resource is the GCE instance
    series.resource.labels["project_id"] = PROJECT_ID
    series.resource.labels["instance_id"] = resource_labels["instance_id"]
    series.resource.labels["zone"] = resource_labels["zone"]
    
    # Create a Point object
    point = monitoring_v3.types.Point() # Correctly instantiate Point
    
    # Set the value based on value_type
    if value_type == "INT64":
        point.value.int64_value = int(value)
    elif value_type == "DOUBLE":
        point.value.double_value = float(value)
    
    # Set the timestamp for the point
    point.interval.end_time = get_current_timestamp_pb()
    
    # Assign the point (or list of points) to the series
    series.points = [point] # Assign as a list containing the single point
    
    return series

def send_metrics_batch(time_series_list: List[monitoring_v3.types.TimeSeries]):
    """Sends a batch of TimeSeries data to Cloud Monitoring."""
    if not time_series_list:
        logging.info("No time series data to send.")
        return

    # Google Cloud Monitoring API allows up to 200 TimeSeries objects per request,
    # but metadata size per request is also limited.
    batch_size = 20 # Reduced batch size to avoid metadata size limit
    for i in range(0, len(time_series_list), batch_size):
        batch = time_series_list[i:i + batch_size]
        try:
            METRIC_CLIENT.create_time_series(name=PROJECT_NAME, time_series=batch)
            logging.info(f"Successfully sent a batch of {len(batch)} time series data points.")
        except Exception as e:
            logging.error(f"Error sending batch of time series data: {e}")
            # Optionally, log the content of the failed batch for debugging
            # for ts_item in batch:
            #     logging.debug(f"Failed TS: {ts_item.metric.type} Labels: {ts_item.metric.labels} Value: {ts_item.points[0].value if ts_item.points else 'N/A'}")


def report_node_reachability(
    target_node_details: Dict[str, Any], # This is a single node_config from CASSANDRA_NODES
    reachable_status: int, # 1 for success, 0 for failure
    failure_reason: str = ""
):
    """Sends a metric about the reachability of a Cassandra node from the utility server."""
    ts_list = []
    metric_labels = {
        "target_cassandra_node_ip": target_node_details["ip"],
        "target_cassandra_instance_name": target_node_details["instance_name"], 
        "failure_reason": failure_reason if failure_reason else "success"
    }
    # The resource for reachability is the Cassandra node being targeted/reported on
    resource_labels = {
        "instance_id": target_node_details["instance_id"],
        "zone": target_node_details["zone"]
    }
    ts_list.append(create_time_series(
        METRIC_REPORTER_REACHABILITY,
        metric_labels,
        resource_labels,
        reachable_status,
        "INT64"
    ))
    send_metrics_batch(ts_list)

# --- Main Execution ---
def main():
    """Main function to iterate through Cassandra nodes and collect metrics."""
    logging.info(f"Starting Cassandra ring monitoring for project {PROJECT_ID}")
    logging.info(f"Loaded {len(CASSANDRA_NODES)} Cassandra nodes from configuration.")

    all_time_series_to_send = []

    for node_config in CASSANDRA_NODES:
        logging.info(f"--- Processing Cassandra Reporter Node: {node_config['instance_name']} ({node_config['ip']}) ---")
        
        # These labels identify the GCE instance that is providing this 'nodetool ring' view.
        # This becomes the monitored resource for the metrics derived from its output.
        reporter_resource_labels = {
            "instance_id": node_config["instance_id"],
            "zone": node_config["zone"]
        }

        nodetool_command = f"{node_config['nodetool_path']} ring" 
        
        ring_output, cmd_error = execute_command_via_iap(
            PROJECT_ID,
            node_config["zone"],
            node_config["instance_name"],
            node_config["ssh_user"],
            nodetool_command,
            TIMEOUTS["ssh_connect_timeout_sec"],
            TIMEOUTS["command_timeout_sec"]
        )

        if ring_output is None: # Command execution failed
            logging.error(f"Failed to get nodetool ring output from {node_config['instance_name']}. Error: {cmd_error}")
            # Fallback to ping check for the target node itself
            is_pingable = ping_check(node_config["ip"])
            failure_detail = cmd_error if cmd_error else "unknown_iap_ssh_error"
            if not is_pingable:
                failure_detail = "ping_failed" # Overwrite if ping also fails
            
            report_node_reachability(node_config, 0, failure_detail)
            continue # Move to the next Cassandra node

        # If command was successful, report reachability as success
        report_node_reachability(node_config, 1, "success")

        # Parse the successful output
        parsed_ring_data = parse_nodetool_ring_output(
            ring_output,
            node_config["ip"], # This is the reporter_node_ip
            node_config["instance_id"], # Pass reporter's GCE instance ID
            node_config["zone"] # Pass reporter's GCE zone
        )

        if not parsed_ring_data:
            logging.warning(f"No node data parsed from nodetool ring output of {node_config['instance_name']}.")
            continue

        logging.info(f"Parsed {len(parsed_ring_data)} observed nodes from {node_config['instance_name']}'s ring output.")

        for observed_node in parsed_ring_data:
            # Metric Labels for individual observed nodes
            node_metric_labels = {
                "reporter_cassandra_node_ip": observed_node["reporter_node_ip"], # IP of the node that ran 'nodetool ring'
                "observed_node_address": observed_node["observed_node_address"], # IP of the node listed in the ring output
                "observed_node_datacenter": observed_node["observed_node_datacenter"],
                "observed_node_rack": observed_node["observed_node_rack"],
                "observed_node_actual_status": observed_node["observed_node_actual_status"],
                "observed_node_actual_state": observed_node["observed_node_actual_state"],
            }

            # 1. Health Status Metric
            all_time_series_to_send.append(create_time_series(
                METRIC_NODE_HEALTH,
                node_metric_labels,
                reporter_resource_labels, # Resource is the GCE instance that reported this view
                observed_node["health_value"],
                "INT64"
            ))

            # 2. Effective Ownership Metric
            all_time_series_to_send.append(create_time_series(
                METRIC_NODE_OWNERSHIP,
                node_metric_labels, 
                reporter_resource_labels,
                observed_node["effective_ownership_percentage"],
                "DOUBLE"
            ))

            # 3. Load Bytes Metric
            all_time_series_to_send.append(create_time_series(
                METRIC_NODE_LOAD_BYTES,
                node_metric_labels, 
                reporter_resource_labels,
                observed_node["load_bytes"],
                "INT64"
            ))
            
            # 4. Load Percentage of Total (calculated per reporter's view)
            all_time_series_to_send.append(create_time_series(
                METRIC_NODE_LOAD_PERCENTAGE,
                node_metric_labels, 
                reporter_resource_labels,
                observed_node["load_percentage_of_total"],
                "DOUBLE"
            ))

    # Send all collected time series data in batches
    if all_time_series_to_send:
        send_metrics_batch(all_time_series_to_send)
    else:
        logging.info("No metrics were generated to send.")

    logging.info("Cassandra ring monitoring run completed.")

if __name__ == "__main__":
    # --- Pre-requisites ---
    # 1. Create a 'config.yaml' file in the same directory as this script,
    #    or update CONFIG_FILE_PATH.
    #    YAML common_cassandra_settings should now only require: ssh_user, nodetool_path
    #    Ensure nodetool_path is the FULL path to the nodetool EXECUTABLE, not a directory.
    # 2. Ensure 'gcloud' CLI is installed and authenticated on the utility server.
    # 3. The service account running this script needs appropriate IAM permissions (see docs).
    #    The ssh_user on Cassandra VMs must be able to run nodetool_path directly.
    # 4. Install PyYAML and google-cloud-monitoring:
    #    pip install PyYAML google-cloud-monitoring
    main()

