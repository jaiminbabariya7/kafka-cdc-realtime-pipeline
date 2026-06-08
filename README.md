# Real-Time Stock Monitoring & Alert System

![Python](https://img.shields.io/badge/Python-3.10+-blue?logo=python)
![GCP](https://img.shields.io/badge/GCP-Pub%2FSub%20%7C%20BigQuery-4285F4?logo=googlecloud)
![Flask](https://img.shields.io/badge/Flask-Dashboard-lightgrey?logo=flask)
![Tests](https://img.shields.io/badge/tests-passing-brightgreen)
![License](https://img.shields.io/badge/License-MIT-green)

> Real-time stock price monitoring with configurable threshold alerts: live price ingestion → GCP Pub/Sub → BigQuery storage → anomaly detection → Flask web dashboard with user authentication.

## Architecture
```
Stock Price API (Alpaca / Yahoo Finance)
        ↓
GCP Pub/Sub (price-events topic)
        ↓
Price Processor
  ├── Store tick data in BigQuery
  ├── Threshold alert check (price > / < target)
  ├── Anomaly detection (z-score on rolling window)
  └── Push alert notifications
        ↓
Flask Web Dashboard
  ├── /login  — user authentication
  ├── /       — live price chart + portfolio view
  └── /dashboard — alerts history + analytics
```

## Features

| Feature | Description |
|---|---|
| Live ingestion | Fetches prices every 5s for configured symbols |
| Threshold alerts | Notify when price crosses user-defined levels |
| Anomaly detection | Z-score based spike/drop detection |
| Web dashboard | Flask app with login, live charts, alert history |
| BigQuery storage | All ticks and alerts stored for analytics |

## Project Structure
```
├── code/
│   ├── fetch_prices.py          # Stock price ingestion
│   ├── process_and_store.py     # Pub/Sub consumer + BigQuery write
│   ├── app.py                   # Flask web application
│   ├── deploy.sh                # Cloud Run deployment
│   └── templates/
│       ├── index.html           # Landing page
│       ├── login.html           # Authentication
│       ├── register.html        # User registration
│       └── dashboard.html       # Live monitoring dashboard
├── requirements.txt
├── architecture                 # System architecture diagram
└── workflow                     # Pipeline workflow
```

## Setup
```bash
git clone https://github.com/jaiminbabariya7/real_time_stock_monitoring_and_alert_system
cd real_time_stock_monitoring_and_alert_system && pip install -r requirements.txt
export ALPACA_API_KEY=your-key
export ALPACA_SECRET_KEY=your-secret
export GCP_PROJECT_ID=your-project
python code/fetch_prices.py &        # start ingestion
python code/app.py                   # start dashboard on :5000
```

## Skills Demonstrated
`Python` · `Flask` · `GCP Pub/Sub` · `BigQuery` · `Real-Time Ingestion` · `Anomaly Detection` · `Web Dashboard` · `Cloud Run`
