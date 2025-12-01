# Database Reliability & Automation 🗄️ 🤖

![Status](https://img.shields.io/badge/Status-Active-success?style=for-the-badge)
![Focus](https://img.shields.io/badge/Focus-Database_Reliability_Engineering-orange?style=for-the-badge)

## 👋 Overview
Welcome to my database automation repository. This collection focuses on **Database Reliability Engineering (DBRE)** tools designed to improve the observability, security, and maintenance of stateful workloads on Google Cloud.

The goal of these scripts is to move away from manual "ssh-and-check" workflows toward **Agentless Observability** and **Zero-Trust Administration**.

## 📂 Project Index

| Automation Tool | Stack | Description |
| :--- | :--- | :--- |
| **[Cassandra Node Monitor](./google-cloud-cassandra-monitor-status)** | Python, GCP IAP | An agentless utility that tunnels into private GCE instances via Identity-Aware Proxy to extract `nodetool` metrics and push them to Cloud Monitoring. |

## 💻 Technologies Used
* **Databases:** Apache Cassandra (NoSQL)
* **Cloud Infrastructure:** Google Compute Engine (GCE), Identity-Aware Proxy (IAP)
* **Observability:** Google Cloud Monitoring (Custom Metrics)
* **Scripting:** Python 3.9+, Bash

## 🏗 Architecture Principles
This repository prioritizes **Security & Minimization**:
1.  **Agentless Design:** We avoid installing custom agents on DB nodes to keep the OS clean and stable.
2.  **Zero Trust Networking:** No public IPs are used. Connectivity is established via **IAP Tunnels** (TLS) authenticated by IAM.
3.  **Structured Metrics:** CLI outputs (like `nodetool ring`) are parsed into structured integers for easier alerting and graphing.

---
*Created and maintained by [SRyanBrenton](https://github.com/SRyanBrenton)*
