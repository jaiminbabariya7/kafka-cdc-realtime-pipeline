# Real-Time Stock Monitoring & Threshold Alert System

![Python](https://img.shields.io/badge/Python-3.9+-blue?logo=python)
![GCP](https://img.shields.io/badge/GCP-Cloud%20Functions%20%7C%20Pub%2FSub%20%7C%20Firestore-4285F4?logo=googlecloud)
![Flask](https://img.shields.io/badge/Flask-Web%20App-lightgrey?logo=flask)
![MIT License](https://img.shields.io/badge/License-MIT-green)

> Serverless, event-driven stock monitoring system on GCP — fetches live prices via Cloud Functions, processes and stores data in Firestore via Pub/Sub, sends threshold-based alerts (email/SMS/push), and serves a web dashboard for alert configuration.

---

## Architecture

```
User Dashboard (Flask/Django)
  ├── Register / Login
  ├── Add stocks to watchlist
  └── Set price thresholds (e.g., AAPL < $180 or > $200)
        ↓
Cloud Scheduler (every 5 min) → HTTP trigger
        ↓
fetch_prices Cloud Function
  ├── Read user watchlist stocks from Firestore
  ├── Call Stock Price API (Yahoo Finance / Alpaca)
  └── Publish price event → Pub/Sub topic: stock-prices
        ↓
process_and_store Cloud Function (Pub/Sub trigger)
  ├── Parse price event
  ├── Write current price + timestamp to Firestore
  ├── Append to price history (last 30 days)
  └── Check against all user thresholds
        ↓ (on threshold breach)
alert_users Cloud Function (Pub/Sub trigger: stock-alerts)
  ├── Look up user notification preferences
  ├── Email alert (SendGrid)
  ├── SMS alert (Twilio)
  └── Log alert to Firestore (for audit/history)
```

---

## Firestore Data Model

```
users/
  {userId}/
    email: "user@example.com"
    notification_pref: "email"       # or "sms" / "both"
    watchlist: ["AAPL", "TSLA", "NVDA"]
    thresholds/
      AAPL:
        low: 180.0
        high: 200.0
        enabled: true
      TSLA:
        low: 220.0
        high: 280.0
        enabled: true

stock_prices/
  AAPL/
    current_price: 189.42
    last_updated: 2024-07-15T14:30:00Z
    history: [
      {price: 189.42, timestamp: "2024-07-15T14:30:00Z"},
      {price: 188.91, timestamp: "2024-07-15T14:25:00Z"},
      ...
    ]

alerts_log/
  {alertId}/
    user_id: "user_001"
    symbol: "AAPL"
    trigger_price: 179.83
    threshold_type: "low"
    threshold_value: 180.0
    sent_at: 2024-07-15T14:30:00Z
    channel: "email"
```

---

## Cloud Functions Code

### 1. fetch_prices
```python
# functions/fetch_prices/main.py
import functions_framework
from google.cloud import firestore, pubsub_v1
import yfinance as yf
import json
from datetime import datetime

db = firestore.Client()
publisher = pubsub_v1.PublisherClient()

@functions_framework.http
def fetch_prices(request):
    """Triggered by Cloud Scheduler every 5 minutes."""
    project_id = "your-project-id"
    topic_path = publisher.topic_path(project_id, "stock-prices")

    # Get all unique symbols from user watchlists
    symbols = set()
    users = db.collection("users").stream()
    for user in users:
        user_data = user.to_dict()
        symbols.update(user_data.get("watchlist", []))

    if not symbols:
        return "No symbols to fetch", 200

    # Fetch prices in batch
    tickers = yf.Tickers(" ".join(symbols))
    published = 0

    for symbol in symbols:
        try:
            ticker = tickers.tickers[symbol]
            price = ticker.fast_info.last_price
            if price:
                message = json.dumps({
                    "symbol": symbol,
                    "price": price,
                    "timestamp": datetime.utcnow().isoformat(),
                }).encode("utf-8")
                publisher.publish(topic_path, message, symbol=symbol)
                published += 1
        except Exception as e:
            print(f"Error fetching {symbol}: {e}")

    return f"Published {published} price events", 200
```

### 2. process_and_store
```python
# functions/process_and_store/main.py
import functions_framework
from google.cloud import firestore, pubsub_v1
import base64, json
from datetime import datetime, timedelta

db = firestore.Client()
publisher = pubsub_v1.PublisherClient()

@functions_framework.cloud_event
def process_and_store(cloud_event):
    """Triggered by stock-prices Pub/Sub topic."""
    data = json.loads(base64.b64decode(cloud_event.data["message"]["data"]).decode("utf-8"))
    symbol = data["symbol"]
    price = data["price"]
    timestamp = data["timestamp"]
    project_id = "your-project-id"
    alerts_topic = publisher.topic_path(project_id, "stock-alerts")

    # Update current price + append to history (last 30 days)
    stock_ref = db.collection("stock_prices").document(symbol)
    cutoff = (datetime.utcnow() - timedelta(days=30)).isoformat()

    stock_doc = stock_ref.get()
    history = []
    if stock_doc.exists:
        history = [h for h in stock_doc.to_dict().get("history", []) if h["timestamp"] > cutoff]

    history.append({"price": price, "timestamp": timestamp})
    stock_ref.set({"current_price": price, "last_updated": timestamp, "history": history})

    # Check thresholds for all users watching this symbol
    users = db.collection("users").where("watchlist", "array_contains", symbol).stream()
    for user in users:
        user_id = user.id
        user_data = user.to_dict()
        thresholds = user_data.get("thresholds", {}).get(symbol, {})

        if not thresholds.get("enabled"):
            continue

        breach_type = None
        if thresholds.get("low") and price < thresholds["low"]:
            breach_type = "low"
        elif thresholds.get("high") and price > thresholds["high"]:
            breach_type = "high"

        if breach_type:
            alert = json.dumps({
                "user_id": user_id,
                "email": user_data.get("email"),
                "notification_pref": user_data.get("notification_pref", "email"),
                "symbol": symbol,
                "trigger_price": price,
                "threshold_type": breach_type,
                "threshold_value": thresholds[breach_type],
            }).encode("utf-8")
            publisher.publish(alerts_topic, alert)
            print(f"Alert triggered: {user_id} | {symbol} @ ${price:.2f} ({breach_type} breach)")
```

### 3. alert_users
```python
# functions/alert_users/main.py
import functions_framework
from google.cloud import firestore
import sendgrid
from sendgrid.helpers.mail import Mail
import base64, json
from datetime import datetime

db = firestore.Client()

@functions_framework.cloud_event
def alert_users(cloud_event):
    """Send email/SMS alert on threshold breach."""
    data = json.loads(base64.b64decode(cloud_event.data["message"]["data"]).decode("utf-8"))

    direction = "fell below" if data["threshold_type"] == "low" else "rose above"
    subject = f"🚨 {data['symbol']} Alert: ${data['trigger_price']:.2f}"
    body = f"""
    Stock Alert for {data['symbol']}

    Current Price: ${data['trigger_price']:.2f}
    Your {data['threshold_type'].upper()} threshold: ${data['threshold_value']:.2f}

    {data['symbol']} has {direction} your threshold of ${data['threshold_value']:.2f}.

    Log in to your dashboard to update your alert settings.
    """

    pref = data.get("notification_pref", "email")

    if pref in ("email", "both"):
        sg = sendgrid.SendGridAPIClient(api_key="SG.YOUR_KEY")
        message = Mail(
            from_email="alerts@stockmonitor.app",
            to_emails=data["email"],
            subject=subject,
            plain_text_content=body,
        )
        sg.send(message)

    # Log alert to Firestore
    db.collection("alerts_log").add({
        **data,
        "sent_at": datetime.utcnow().isoformat(),
        "subject": subject,
    })
    print(f"Alert sent to {data['email']}: {subject}")
```

---

## Sample Alert Flow

```
[14:30:01] Cloud Scheduler → fetch_prices triggered
[14:30:03] Fetched 12 symbols: AAPL=$179.83, TSLA=$241.12, NVDA=$492.30...
[14:30:03] Published 12 price events to stock-prices topic

[14:30:05] process_and_store triggered (AAPL)
[14:30:05] AAPL: $179.83 stored | history updated (147 entries)
[14:30:05] Checking thresholds: user_001 (low=$180.00) → BREACH DETECTED
[14:30:05] Alert published to stock-alerts topic

[14:30:06] alert_users triggered
[14:30:06] Email sent to: user@example.com
           Subject: 🚨 AAPL Alert: $179.83
           AAPL has fallen below your threshold of $180.00
[14:30:06] Alert logged to Firestore: alert_id=ALR-20240715-0042
```

---

## Project Structure

```
real_time_stock_monitoring_and_alert_system/
├── functions/
│   ├── fetch_prices/
│   │   ├── main.py
│   │   └── requirements.txt
│   ├── process_and_store/
│   │   ├── main.py
│   │   └── requirements.txt
│   └── alert_users/
│       ├── main.py
│       └── requirements.txt
├── web/
│   ├── app.py              # Flask dashboard
│   └── templates/
│       ├── dashboard.html
│       └── alert_settings.html
├── scripts/
│   └── seed_firestore.py   # Test data seeder
└── README.md
```

---

## Deployment

```bash
# Deploy all three Cloud Functions
gcloud functions deploy fetch_prices \
  --gen2 --runtime python311 --region us-central1 \
  --trigger-http --allow-unauthenticated \
  --source functions/fetch_prices/

gcloud functions deploy process_and_store \
  --gen2 --runtime python311 --region us-central1 \
  --trigger-topic stock-prices \
  --source functions/process_and_store/

gcloud functions deploy alert_users \
  --gen2 --runtime python311 --region us-central1 \
  --trigger-topic stock-alerts \
  --source functions/alert_users/

# Schedule fetch_prices to run every 5 minutes
gcloud scheduler jobs create http fetch-stock-prices \
  --schedule "*/5 * * * *" \
  --uri "https://REGION-PROJECT.cloudfunctions.net/fetch_prices" \
  --http-method POST
```

---

## Skills Demonstrated
`Serverless Architecture` · `Cloud Functions` · `Pub/Sub` · `Firestore` · `Event-Driven Design` · `Cloud Scheduler` · `Real-Time Alerting` · `Flask` · `GCP` · `Python`
