# producer.py
#
# PURPOSE: Binance WebSocket -> Kafka producer (Part 1 – Ingestion)
#
# STEPS TO IMPLEMENT:
#
# 1. CONFIGURATION
#    - Read KAFKA_BOOTSTRAP (default "localhost:9092"), KAFKA_TOPIC (default "prix"),
#      and SYMBOLS (default "btcusdt,ethusdt,bnbusdt,solusdt") from environment variables.
#
# 2. BUILD THE BINANCE STREAM URL
#    - Combine all symbols into a single combined stream URL:
#      wss://stream.binance.com:9443/stream?streams=btcusdt@trade/ethusdt@trade/...
#
# 3. CREATE THE KAFKA PRODUCER
#    - Instantiate a confluent_kafka (or kafka-python) Producer pointing at KAFKA_BOOTSTRAP.
#    - Serialize messages as UTF-8 JSON.
#    - Use symbol as the Kafka message key so all ticks for one symbol go to the same partition.
#
# 4. CONNECT TO THE WEBSOCKET
#    - Open a persistent WebSocket connection to the combined Binance stream URL.
#    - Parse each incoming frame as JSON.
#
# 5. PARSE EACH TRADE EVENT INTO THE TICK SCHEMA
#    - Extract: symbol (s), price (p), quantity (q), event_time (T -> ISO-8601 string), trade_id (t).
#    - Build the dict: {"symbol", "price", "quantity", "event_time", "trade_id"}
#
# 6. SEND TO KAFKA
#    - Produce the JSON-serialized tick to KAFKA_TOPIC with key = symbol.
#    - Call producer.flush() periodically (or per message) to avoid buffering delays.
#
# 7. ERROR HANDLING & AUTO-RECONNECT
#    - Catch WebSocket disconnections and network errors.
#    - Implement exponential backoff before each reconnection attempt.
#    - Log all errors with timestamps (use Python logging, not print).
#
# 8. GRACEFUL SHUTDOWN
#    - Handle SIGTERM / KeyboardInterrupt to flush and close the producer cleanly.

