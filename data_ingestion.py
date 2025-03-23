import psycopg2
from apscheduler.schedulers.blocking import BlockingScheduler
import datetime
import yfinance as yf
from kafka import KafkaProducer
import json

# Replace with your PostgreSQL credentials
DB_HOST = "postgres_db"  # or the host where PostgreSQL is running
DB_NAME = "stock_data"  # The name of the database
DB_USER = "postgres"  # Your PostgreSQL username
DB_PASSWORD = "dino2711"  # Your PostgreSQL password

# Stock symbols
stock_symbols = ["AAPL", "GOOGL"]

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',  # Use localhost or the actual IP address of the host machine
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)


def send_to_kafka(symbol, price, timestamp):
    message = {
        'symbol': symbol,
        'price': price,
        'timestamp': timestamp
    }
    producer.send('test-topic', value=message)
    print(f"Sent data to Kafka: {message}")

# Function to create the table in PostgreSQL
def create_table():
    try:
        # Establish connection to PostgreSQL
        conn = psycopg2.connect(host=DB_HOST, database=DB_NAME, user=DB_USER, password=DB_PASSWORD)
        cur = conn.cursor()

        # Create table for stock data
        cur.execute(''' 
            CREATE TABLE IF NOT EXISTS stocks (
                id SERIAL PRIMARY KEY,
                symbol TEXT NOT NULL,
                price REAL NOT NULL,
                timestamp TEXT NOT NULL
            );
        ''')

        # Commit changes and close connection
        conn.commit()
        cur.close()
        conn.close()
        print("Table created successfully!")
    except Exception as e:
        print(f"Error creating table: {e}")


# Function to insert stock data into PostgreSQL
def insert_stock_data(symbol, price, timestamp):
    try:
        conn = psycopg2.connect(host=DB_HOST, database=DB_NAME, user=DB_USER, password=DB_PASSWORD)
        cur = conn.cursor()

        # Check if this timestamp already exists for the stock symbol
        cur.execute('''
            SELECT 1 FROM stocks WHERE symbol = %s AND timestamp = %s;
        ''', (symbol, timestamp))

        if cur.fetchone() is None:  # Only insert if timestamp is not found
            cur.execute('''
                INSERT INTO stocks (symbol, price, timestamp) 
                VALUES (%s, %s, %s);
            ''', (symbol, price, timestamp))
            conn.commit()
            print(f"Data for {symbol} inserted successfully at {timestamp}!")
        else:
            print(f"Duplicate entry detected for {symbol} at {timestamp}. Skipping insertion.")

        cur.close()
        conn.close()

    except Exception as e:
        print(f"Error inserting data: {e}")


# Function to fetch stock price using yfinance
def fetch_stock_price(symbol):
    stock = yf.Ticker(symbol)
    data = stock.history(period="1d", interval="5m")  # Fetching data for the past day with 5-minute intervals

    if not data.empty:
        latest_data = data.iloc[-1]  # Get the latest data
        price = float(latest_data['Open'])  # Convert NumPy float to standard Python float
        timestamp = latest_data.name.strftime('%Y-%m-%d %H:%M:%S')  # Convert timestamp to string
        return price, timestamp
    else:
        print(f"Failed to fetch data for {symbol}")
        return None, None


# Function to fetch and store data
def fetch_and_store_data():
    for symbol in stock_symbols:
        price, timestamp = fetch_stock_price(symbol)
        if price:
            print(f"Fetched data: {symbol} - {price} at {timestamp}")  # Debugging print
            insert_stock_data(symbol, price, timestamp)
            send_to_kafka(symbol, price, timestamp) # Send data to Kafka
        else:
            print(f"Failed to fetch data for {symbol}")


# Function to be scheduled
def scheduled_task():
    print(f"Running task at {datetime.datetime.now()}")
    fetch_and_store_data()

# Uncomment if you need to create the table (run only once)
# create_table()

# Create a scheduler instance
scheduler = BlockingScheduler()

# Schedule the task every 5 minutes
scheduler.add_job(scheduled_task, 'interval', minutes=5)

# Start the scheduler
scheduler.start()