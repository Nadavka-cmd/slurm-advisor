# Slurm Advisor — Setup Guide

A web-based job advisor for HPC clusters running Slurm and Open OnDemand. Helps researchers pick the right partition, understand pending job reasons, and improve GPU/CPU/memory efficiency — all in plain language.

---

## Table of Contents

1. [Prerequisites](#1-prerequisites)
2. [Installation](#2-installation)
3. [Configuration](#3-configuration)
4. [Systemd Service](#4-systemd-service)
5. [OOD Reverse Proxy Setup](#5-ood-reverse-proxy-setup)
6. [OOD App Tile](#6-ood-app-tile)
7. [Optional: g-seff Integration](#7-optional-g-seff-integration)
8. [Feature Overview](#8-feature-overview)

---

## 1. Prerequisites

- Python 3.9+
- Slurm CLI tools (`squeue`, `sinfo`, `scontrol`, `sacct`, `sacctmgr`) in PATH or configured path
- Open OnDemand (for reverse proxy + PAM auth) — or any other reverse proxy
- Optional: [g-seff](https://github.com/Nadavka-cmd/g-seff) for GPU efficiency recommendations

---

## 2. Installation

```bash
sudo mkdir -p /opt/slurm-advisor/app/routers
sudo mkdir -p /opt/slurm-advisor/templates

sudo cp main.py /opt/slurm-advisor/
sudo cp app/routers/*.py /opt/slurm-advisor/app/routers/
sudo cp templates/index.html /opt/slurm-advisor/templates/
sudo cp slurm_advisor_config.yaml /opt/slurm-advisor/
sudo cp manifest.yml /opt/slurm-advisor/

sudo python3 -m venv /opt/slurm-advisor/venv
sudo /opt/slurm-advisor/venv/bin/pip install -r requirements.txt
```

---

## 3. Configuration

Edit `slurm_advisor_config.yaml` to match your cluster. This file drives everything — partitions, thresholds, and admin access.

### Partitions

Each partition entry defines its hardware and tier:

```yaml
partitions:
  shared_a6000:
    vram_gb: 48
    tier: high_vram        # high_vram | moderate | low
    rec_cpu: 48
    max_cpu: 48
    rec_mem_gb: 120
    max_mem_gb: 120
    max_gpu: 4
    notes: "Highest VRAM on cluster. Reserve for jobs needing >24 GB."
  shared_a5000:
    vram_gb: 24
    tier: moderate
    # ...
```

The `tier` field drives the guided wizard recommendations:
- `high_vram` — suggested only when user indicates >24 GB VRAM need
- `moderate` — default recommendation for most workloads
- `low` — suggested for small models and debugging

### Efficiency & Admin

```yaml
efficiency:
  flag_threshold_pct: 30   # GPU util % below which jobs are flagged
  days_lookback: 15        # lookback window (match your Prometheus retention)
  admin_group: hpc_admins  # AD/Linux group with access to cluster-wide views
```

### Pressure Thresholds

```yaml
pressure:
  low_max: 0       # <= 0 pending jobs = Low pressure
  medium_max: 3    # <= 3 = Medium
  high_max: 8      # <= 8 = High, > 8 = Very High
```

Tune these to your cluster size. Partition pressure data is cached for 60 seconds.

---

## 4. Systemd Service

```bash
sudo cp slurm-advisor.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now slurm-advisor
sudo systemctl status slurm-advisor
```

The service runs on port `8767` by default. Edit `slurm-advisor.service` to change the port or working directory.

---

## 5. OOD Reverse Proxy Setup

Add to `/etc/ood/config/ood_portal.yml`:

```yaml
- '<Location /slurm-advisor>'
- '  AuthType Basic'
- '  AuthName "HPC"'
- '  AuthBasicProvider PAM'
- '  AuthPAMService ood'
- '  Require valid-user'
- '  RequestHeader set X-Remote-User %{REMOTE_USER}e env=REMOTE_USER'
- '  ProxyPass http://127.0.0.1:8767/slurm-advisor'
- '  ProxyPassReverse http://127.0.0.1:8767/slurm-advisor'
- '</Location>'
```

Regenerate and reload OOD:

```bash
sudo /opt/ood/ood-portal-generator/sbin/update_ood_portal
sudo cp /etc/httpd/conf.d/ood-portal.conf.new /etc/httpd/conf.d/ood-portal.conf
sudo systemctl reload httpd
```

---

## 6. OOD App Tile

Create `/var/www/ood/apps/sys/slurm-advisor/manifest.yml`:

```yaml
---
name: "Slurm Advisor"
category: "Jobs"
subcategory: "Advisor"
icon: "fa://lightbulb"
description: "Partition recommendations, job efficiency, and queue insights"
url: "/slurm-advisor/"
new_window: true
```

---

## 7. Optional: g-seff Integration

The Efficiency tab fetches GPU utilization data from [g-seff](https://github.com/Nadavka-cmd/g-seff) running on `http://127.0.0.1:8766`. If g-seff is not deployed, the Efficiency tab degrades gracefully — it shows a warning banner instead of crashing.

To enable full efficiency features, deploy g-seff and ensure it is reachable at that address.

---

## 8. Feature Overview

| Tab | Feature |
|-----|---------|
| **Dashboard** | Active jobs, recent completed jobs, quick partition pressure overview |
| **Partitions** | Per-partition queue pressure, VRAM, wait time estimate |
| **Advisor → From Job** | Pick a recent job — get recommendations based on actual GPU/CPU/memory usage |
| **Advisor → Guided** | Step-by-step wizard: task type → model size → partition recommendation |
| **Pending Reasons** | Click any pending job to get a plain-English explanation and fix suggestion |
| **Efficiency** | Per-user GPU/CPU/memory efficiency report with actionable recommendations (requires g-seff) |
