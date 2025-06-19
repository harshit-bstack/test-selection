from flask import Flask, jsonify, request
from db.database import get_db_connection

app = Flask(__name__)

@app.route('/')
def index():
    return "Hello, World!"

# Example: Get all items from a table
@app.route('/items', methods=['GET'])
def get_items():
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute('SELECT * FROM items;')
    items = cur.fetchall()
    cur.close()
    conn.close()
    return jsonify(items)

# Example: Add an item to a table
@app.route('/items', methods=['POST'])
def add_item():
    new_item = request.get_json()
    name = new_item['name']
    
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute('INSERT INTO items (name) VALUES (%s)', (name,))
    conn.commit()
    cur.close()
    conn.close()
    
    return jsonify(new_item)

@app.route('/repo_pr_details', methods=['POST'])
def add_repo_pr_details():
    details = request.get_json()
    repo_name = details['repo_name']
    pr_link = details['pr_link']
    date_of_pr = details['date_of_pr']
    list_of_authors = details['list_of_authors']
    diff_s3_link = details['diff_s3_link']
    
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute('INSERT INTO repo_pr_details (repo_name, pr_link, date_of_pr, list_of_authors, diff_s3_link) VALUES (%s, %s, %s, %s, %s)',
                (repo_name, pr_link, date_of_pr, list_of_authors, diff_s3_link))
    conn.commit()
    cur.close()
    conn.close()
    
    return jsonify(details)

@app.route('/repo_pr_details', methods=['GET'])
def get_all_repo_pr_details():
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute('SELECT * FROM repo_pr_details;')
    details = cur.fetchall()
    cur.close()
    conn.close()
    return jsonify(details)

@app.route('/repo_pr_details/<repo_name>/<path:pr_link>', methods=['GET'])
def get_repo_pr_details(repo_name, pr_link):
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute('SELECT * FROM repo_pr_details WHERE repo_name = %s AND pr_link = %s;', (repo_name, pr_link))
    details = cur.fetchone()
    cur.close()
    conn.close()
    return jsonify(details)

@app.route('/pr_to_files_changed_mapping', methods=['POST'])
def add_pr_to_files_changed_mapping():
    mapping = request.get_json()
    pr_link = mapping['pr_link']
    files_paths_changed = mapping['files_paths_changed']
    
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute('INSERT INTO pr_to_files_changed_mapping (pr_link, files_paths_changed) VALUES (%s, %s)',
                (pr_link, files_paths_changed))
    conn.commit()
    cur.close()
    conn.close()
    
    return jsonify(mapping)

@app.route('/pr_to_files_changed_mapping/<path:pr_link>', methods=['GET'])
def get_pr_to_files_changed_mapping(pr_link):
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute('SELECT * FROM pr_to_files_changed_mapping WHERE pr_link = %s;', (pr_link,))
    mappings = cur.fetchall()
    cur.close()
    conn.close()
    return jsonify(mappings)

@app.route('/pr_to_test_runs_mapping', methods=['POST'])
def add_pr_to_test_runs_mapping():
    mapping = request.get_json()
    pr_link = mapping['pr_link']
    test_case_file_path = mapping['test_case_file_path']
    test_case_name = mapping['test_case_name']
    actual_result = mapping['actual_result']
    
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute('INSERT INTO pr_to_test_runs_mapping (pr_link, test_case_file_path, test_case_name, actual_result) VALUES (%s, %s, %s, %s)',
                (pr_link, test_case_file_path, test_case_name, actual_result))
    conn.commit()
    cur.close()
    conn.close()
    
    return jsonify(mapping)

@app.route('/pr_to_test_runs_mapping/<path:pr_link>', methods=['GET'])
def get_pr_to_test_runs_mapping(pr_link):
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute('SELECT * FROM pr_to_test_runs_mapping WHERE pr_link = %s;', (pr_link,))
    mappings = cur.fetchall()
    cur.close()
    conn.close()
    return jsonify(mappings)

@app.route('/predicted_result', methods=['POST'])
def add_predicted_result():
    result = request.get_json()
    pr_link = result['pr_link']
    test_case_file_path = result['test_case_file_path']
    test_case_name = result['test_case_name']
    predicted_result = result['predicted_result']
    checkpoint = result['checkpoint']
    
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute('INSERT INTO predicted_result (pr_link, test_case_file_path, test_case_name, predicted_result, checkpoint) VALUES (%s, %s, %s, %s, %s)',
                (pr_link, test_case_file_path, test_case_name, predicted_result, checkpoint))
    conn.commit()
    cur.close()
    conn.close()
    
    return jsonify(result)

@app.route('/predicted_result/<path:pr_link>', methods=['GET'])
def get_predicted_result(pr_link):
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute('SELECT * FROM predicted_result WHERE pr_link = %s;', (pr_link,))
    results = cur.fetchall()
    cur.close()
    conn.close()
    return jsonify(results)

@app.route('/checkpoint_details', methods=['POST'])
def add_checkpoint_details():
    details = request.get_json()
    repo_name = details['repo_name']
    date_of_checkpoint = details['date_of_checkpoint']
    meta_data = details.get('meta_data')

    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute('INSERT INTO checkpoint_details (repo_name, date_of_checkpoint, meta_data) VALUES (%s, %s, %s) RETURNING checkpoint',
                (repo_name, date_of_checkpoint, meta_data))
    new_checkpoint_id = cur.fetchone()[0]
    conn.commit()
    cur.close()
    conn.close()

    details['checkpoint'] = new_checkpoint_id
    return jsonify(details)

@app.route('/checkpoint_details/<repo_name>', methods=['GET'])
def get_checkpoint_details_by_repo(repo_name):
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute('SELECT * FROM checkpoint_details WHERE repo_name = %s;', (repo_name,))
    details = cur.fetchall()
    cur.close()
    conn.close()
    return jsonify(details)

@app.route('/checkpoint_details/<repo_name>/<int:checkpoint>', methods=['GET'])
def get_checkpoint_detail(repo_name, checkpoint):
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute('SELECT * FROM checkpoint_details WHERE repo_name = %s AND checkpoint = %s;', (repo_name, checkpoint))
    detail = cur.fetchone()
    cur.close()
    conn.close()
    return jsonify(detail)


if __name__ == '__main__':
    app.run(debug=True)
