# Flask API with PostgreSQL

This is a simple Flask API that connects to a PostgreSQL database.

## Setup

1. **Install PostgreSQL:**

   Make sure you have PostgreSQL installed and running on your machine.

2. **Create a database and table:**

   ```sql
   CREATE DATABASE your_db;
   \c your_db;
   CREATE TABLE items (id serial PRIMARY KEY, name VARCHAR(50));
   ```

3. **Install dependencies:**

   ```bash
   pip install -r requirements.txt
   ```

4. **Run the application:**

   ```bash
   python app.py
   ```

The API will be running at `http://127.0.0.1:5000`.
