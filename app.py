from flask import Flask, jsonify, request
from db.database import get_db_connection
import schedule
import time
import threading
from cron.process_data import main as process_data_main

app = Flask(__name__)

@app.route('/')
def index():
    return "Hello, World!"

def run_cron_jobs():
    """Runs the scheduled cron jobs."""
    print("Starting cron job scheduler...")
    # Schedule the process_data_main function to run every 24 hours
    schedule.every(24).hours.do(process_data_main)

    while True:
        schedule.run_pending()
        time.sleep(1)

if __name__ == '__main__':
    # Start the cron job scheduler in a background thread
    cron_thread = threading.Thread(target=run_cron_jobs)
    cron_thread.daemon = True
    cron_thread.start()
    process_data_main()
    # Run the Flask app
    app.run(debug=True, use_reloader=False)
