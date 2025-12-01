# google-cloud-cassandra-monitor-status 👁️ 📊

![Python](https://img.shields.io/badge/Python-3.9-blue?style=for-the-badge&logo=python&logoColor=white)
![Cassandra](https://img.shields.io/badge/Apache_Cassandra-Nodetool-skyblue?style=for-the-badge&logo=apache-cassandra&logoColor=white)
![GCP](https://img.shields.io/badge/Google_Cloud-Monitoring_%26_IAP-red?style=for-the-badge&logo=google-cloud&logoColor=white)

## 📖 Overview
This utility provides agentless observability for Apache Cassandra clusters running on Google Compute Engine (GCE). 

Instead of installing monitoring agents directly on the database nodes (which increases overhead and security surface area), this script runs on a centralized "Utility Server." It securely tunnels into the cluster using **Identity-Aware Proxy (IAP)**, executes the native `nodetool ring` command, parses the cluster state, and pushes real-time health metrics to **Google Cloud Monitoring**.

This script can run on a central utility gce server, connects to specified Cassandra GCE VMs via IAP, executes 'nodetool ring', parses the output, and sends metrics to Google Cloud Monitoring. It loads configuration from a 'config.yaml' file.

## 🏗 Architecture & Security
This solution prioritizes **Zero Trust** security principles. No public IP addresses or open firewall ports (22) are required on the database nodes.

`Utility Server` ➔ `IAP Tunnel (TLS)` ➔ `Cassandra Node (Internal IP)` ➔ `Exec: nodetool ring` ➔ `Parse & Push to Stackdriver`

## ✨ Key Features
* **Secure Connectivity:** Utilizes GCP Identity-Aware Proxy (IAP) for SSH tunneling, eliminating the need for VPNs or Bastion hosts with public IPs.
* **Custom Metrics:** Converts text-based CLI output into structured **TimeSeries data** in Google Cloud Monitoring.
* **Status Mapping:** Automatically maps Cassandra string statuses (`Up/Down`, `Normal/Leaving`) to integer values for easy graphing and alerting.
* **Configurable:** Driven by a `config.yaml` file to easily manage multiple clusters or environments.

## ⚙️ Configuration
The script relies on a `config.yaml` file to define the target environment.

**Example `config.yaml`:**
```yaml
project_id: "my-gcp-project-id"
zone: "us-central1-a"
instance_name: "cassandra-node-01"
metric_type_prefix: "[custom.googleapis.com/cassandra/node_status](https://custom.googleapis.com/cassandra/node_status)"
cassandra_home: "/opt/cassandra"
```

## 📊 Metrics & Dashboarding
The script parses the standard `nodetool ring` output and converts it into the following custom metrics:

**1. Metric: `custom.googleapis.com/cassandra/status`**
* Tracks the network connectivity of the node.
* **1** = `Up` (Healthy)
* **0** = `Down` (Unreachable)

**2. Metric: `custom.googleapis.com/cassandra/state`**
* Tracks the lifecycle state of the node.
* **1** = `Normal` (Active ring member)
* **2** = `Leaving` (Decommissioning)
* **3** = `Joining` (Bootstrapping)
* **0** = `Unknown/Error`

*Note: These integer mappings allow you to set simple alert thresholds (e.g., "Alert if Status < 1 for 5 minutes").*

## 🛠️ Prerequisites
* **Python 3.x** installed on the Utility Server.
* **Google Cloud SDK (gcloud)** installed and authenticated.
* **IAM Permissions:**
    * The Utility Server Service Account needs:
        * `monitoring.metricWriter` (To push metrics)
        * `iap.tunnelResourceAccessor` (To connect via IAP)
        * `compute.instanceAdmin.v1` (To initiate SSH)

## 🚀 Usage & Deployment

### 1. Install Dependencies
```bash
pip install -r requirements.txt
```
*Requires `google-cloud-monitoring`, `pyyaml`*

### 2. Manual Execution
Test the connection and metric payload by running manually:
```bash
python3 main.py --config config.yaml
```

### 3. Automated Monitoring (Cron)
To ensure continuous observability, schedule the script via Crontab on the Utility Server.

**Example (Run every 5 minutes):**
```bash
*/5 * * * * /usr/bin/python3 /opt/ops-scripts/cassandra-monitor/main.py >> /var/log/cassandra-monitor.log 2>&1
```

## 🔍 Troubleshooting
* **IAP Connection Failed:** Ensure the firewall rule `allow-ingress-from-iap` (IP range `35.235.240.0/20`) exists on the Cassandra network VPC.
* **Nodetool Not Found:** Check that the `cassandra_home` path in `config.yaml` points to the correct binary location on the remote server.

---
*Created and maintained by Shaun Brenton*
