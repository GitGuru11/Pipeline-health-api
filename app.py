from flask import Flask, jsonify, request
from dotenv import load_dotenv
import os
import clickhouse_connect
from copy import deepcopy
from flask_cors import CORS

# Load environment variables from the .env file
load_dotenv()
app = Flask(__name__)

CORS(app)

# Get credentials and URL from environment variables
CLICKHOUSE_URL = os.getenv('CLICKHOUSE_URL')
CLICKHOUSE_USER = os.getenv('CLICKHOUSE_USER')
CLICKHOUSE_PASSWORD = os.getenv('CLICKHOUSE_PASSWORD')
CLICKHOUSE_DB = os.getenv('CLICKHOUSE_DB')

# Establish connection to ClickHouse
client = clickhouse_connect.get_client(
    host=CLICKHOUSE_URL,
    port=8443,  # Default ClickHouse HTTPS port
    username=CLICKHOUSE_USER,
    password=CLICKHOUSE_PASSWORD,
    database=CLICKHOUSE_DB,
    interface='https',
    # secure='True'
)

if not CLICKHOUSE_URL or not CLICKHOUSE_USER or not CLICKHOUSE_PASSWORD or not CLICKHOUSE_DB:
    raise ValueError("Missing required environment variables for ClickHouse connection")

# API Endpoint: /table_data
# Method: GET
# Parameters: 'parent_node' (query parameter, string)
# Description: This endpoint fetches child data associated with 'parent_node' id
# Response: A JSON object with data or an error message.
@app.route('/table_data', methods=['GET'])
def get_table_data():
    parent_node = request.args.get('parent_node')
    print(parent_node)
    if not parent_node:
        return jsonify({"status": "Error", "message": "Parent Node parameter is missing"}), 400

    try:
        query = "SELECT * FROM 'overview_copy' WHERE parent = %s"
        result = client.query(query, (parent_node, )).result_rows
        fields = (
            'node',
            'coverage_4rq', 'coverage_threshold', 'qualification_4rq', 'qualification_threshold', 'maturity_4rq', 'maturity_threshold',
            'spread_percent', 'quality_percent',
            'coverage_score', 'qualification_score', 'maturity_score', 'spread_score', 'quality_score', 'total_score',
            'spread_threshold', 'quality_threshold',
            'coverage_score_status', 'qualification_score_status', 'maturity_score_status', 'spread_score_status', 'quality_score_status', 'total_score_status',
            '2rq_open_pipe', '2rq_threshold', 'openpipe_gap_final', 'qualified_gap_final', 'mature_gap_final',
            'coverage_insights', 'qualification_insights', 'maturity_insights', 'spread_insights', 'quality_insights',
            'parent'
        )
        response = [dict(zip(fields, values)) for values in result]
        return jsonify(response), 200

    except Exception as e:
        return jsonify({"status": "Data not found"}), 404


# API Endpoint: /chart_data
# Method: GET
# Parameters: 'node' (query parameter, string)
# Description: This endpoint fetches chart data associated with 'node' id
# Response: A JSON object with data or an error message.
@app.route('/chart_data', methods=['GET'])
def get_chart_data():
    node = request.args.get('node')
    print(node)
    if not node:
        return jsonify({"status": "Error", "message": "Node parameter is missing"}), 400

    try:
        query = "SELECT * FROM 'overview_copy' WHERE node = %s"
        result = client.query(query, (node, )).result_rows
        fields = (
            'node',
            'coverage_4rq', 'coverage_threshold', 'qualification_4rq', 'qualification_threshold', 'maturity_4rq', 'maturity_threshold',
            'spread_percent', 'quality_percent',
            'coverage_score', 'qualification_score', 'maturity_score', 'spread_score', 'quality_score', 'total_score',
            'spread_threshold', 'quality_threshold',
            'coverage_score_status', 'qualification_score_status', 'maturity_score_status', 'spread_score_status', 'quality_score_status', 'total_score_status',
            '2rq_open_pipe', '2rq_threshold', 'openpipe_gap_final', 'qualified_gap_final', 'mature_gap_final',
            'coverage_insights', 'qualification_insights', 'maturity_insights', 'spread_insights', 'quality_insights',
            'parent'
        )
        response = []
        for val in result:
            table_values = dict(zip(fields, val))
            res_keys = ['Coverage', 'Qualification', 'Maturity', 'Spread', 'Quality']
            res_value_format = {
                "values": "",
                "title": "",
                "description": {
                    "content": "",
                    "boldLabel": [],
                    "linkLabel": [],
                    "linkAction": []
                }
            }
            res = {}
            for res_key in res_keys:
                res_value = deepcopy(res_value_format)
                res_value["values"] = table_values[f"{res_key.lower()}_score"]
                # res_values_format["titles"] = table_values[""]
                # res_values_format["titles"] = table_values[""]
                temp = res_value["description"]["content"] = table_values[f"{res_key.lower()}_insights"]
                res_value["description"]["content"] = table_values[f"{res_key.lower()}_insights"]
                # res_values_format["description"]["boldLabel"] =
                # res_values_format["description"]["linkLabel"] =
                # res_values_format["description"]["linkAction"] =
                res[res_key] = res_value
            response.append(res)
        return jsonify(response), 200

    except Exception as e:
        return jsonify({"status": "Data not found"}), 404


# API Endpoint: /add_data
# Method: POST
# Description: This endpoint dynamically adds a new record to the overview_copy table.
# Request Body: JSON containing the fields you want to insert.
# Response: Success or error message.
@app.route('/add_data', methods=['POST'])
def add_data():
    try:
        # Get JSON data from the request body
        data = request.get_json()

        # Define available fields
        fields = (
            'node',
            'coverage_4rq', 'coverage_threshold', 'qualification_4rq', 'qualification_threshold', 'maturity_4rq',
            'maturity_threshold',
            'spread_percent', 'quality_percent',
            'coverage_score', 'qualification_score', 'maturity_score', 'spread_score', 'quality_score', 'total_score',
            'spread_threshold', 'quality_threshold',
            'coverage_score_status', 'qualification_score_status', 'maturity_score_status', 'spread_score_status',
            'quality_score_status', 'total_score_status',
            '2rq_open_pipe', '2rq_threshold', 'openpipe_gap_final', 'qualified_gap_final', 'mature_gap_final',
            'coverage_insights', 'qualification_insights', 'maturity_insights', 'spread_insights', 'quality_insights',
            'parent'
        )

        # Filter out only the fields that are present in the request body
        data_keys = [key for key in fields if key in data]
        data_values = [data[key] for key in data_keys]

        # Prepare dynamic insert query
        query = f"INSERT INTO overview_copy ({', '.join(data_keys)}) VALUES ({', '.join(['%s'] * len(data_keys))})"

        # Execute the insert query
        client.command(query, data_values)

        return jsonify({"status": "Success", "message": "Record added successfully"}), 201

    except Exception as e:
        return jsonify({"status": "Error", "message": str(e)}), 400


# API Endpoint: /update_data
# Method: PUT
# Parameters: 'node' (query parameter, string)
# Description: This endpoint dynamically updates an existing record in the overview_copy table.
# Request Body: JSON containing the fields you want to update.
# Response: Success or error message.
@app.route('/update_data', methods=['PUT'])
def update_data():
    node = request.args.get('node')

    if not node:
        return jsonify({"status": "Error", "message": "Node parameter is missing"}), 400

    try:
        # Get JSON data from the request body
        data = request.get_json()

        # Define available fields
        fields = (
            'node',
            'coverage_4rq', 'coverage_threshold', 'qualification_4rq', 'qualification_threshold', 'maturity_4rq',
            'maturity_threshold',
            'spread_percent', 'quality_percent',
            'coverage_score', 'qualification_score', 'maturity_score', 'spread_score', 'quality_score', 'total_score',
            'spread_threshold', 'quality_threshold',
            'coverage_score_status', 'qualification_score_status', 'maturity_score_status', 'spread_score_status',
            'quality_score_status', 'total_score_status',
            '2rq_open_pipe', '2rq_threshold', 'openpipe_gap_final', 'qualified_gap_final', 'mature_gap_final',
            'coverage_insights', 'qualification_insights', 'maturity_insights', 'spread_insights', 'quality_insights',
            'parent'
        )

        # Filter out only the fields that are present in the request body
        update_fields = [f"{key} = %s" for key in fields if key in data]
        update_values = [data[key] for key in fields if key in data]

        if not update_fields:
            return jsonify({"status": "Error", "message": "No valid fields to update"}), 400

        # Prepare the dynamic update query
        query = f"ALTER TABLE overview_copy UPDATE {', '.join(update_fields)} WHERE node = %s"
        update_values.append(node)

        # Execute the update query
        client.command(query, update_values)

        return jsonify({"status": "Success", "message": f"Record with node {node} updated successfully"}), 200

    except Exception as e:
        return jsonify({"status": "Error", "message": str(e)}), 400


# API Endpoint: /delete_data
# Method: DELETE
# Parameters: 'node' (query parameter, string)
# Description: This endpoint deletes a record from the overview_copy table where the node matches.
# Response: Success or error message.
@app.route('/delete_data', methods=['DELETE'])
def delete_data():
    node = request.args.get('node')

    if not node:
        return jsonify({"status": "Error", "message": "Node parameter is missing"}), 400

    try:
        # Prepare the delete query
        query = "DELETE FROM overview_copy WHERE node = %s"

        # Execute the query
        client.command(query, (node,))

        return jsonify({"status": "Success", "message": f"Record with node {node} deleted successfully"}), 200

    except Exception as e:
        return jsonify({"status": "Error", "message": str(e)}), 400


if __name__ == '__main__':
    app.run()
