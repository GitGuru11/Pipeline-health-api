from flask import Flask, jsonify, request
from dotenv import load_dotenv
import os
import clickhouse_connect
from copy import deepcopy
from flask_cors import CORS
from datetime import datetime

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
            'parent',"node_label"
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
                    "subtitle": "",
                    "boldLabel": [],
                    "linkLabel": [],
                    "linkAction": []
                }
            }
            res = {}
            for res_key in res_keys:
                res_value = deepcopy(res_value_format)
                res_value["values"] = table_values[f"{res_key.lower()}_score"]
                res_value["title"] = res_key
                # res_values_format["titles"] = table_values[""]
                # res_values_format["titles"] = table_values[""]
                temp = res_value["description"]["content"] = table_values[f"{res_key.lower()}_insights"]
                res_value["description"]["content"] = table_values[f"{res_key.lower()}_insights"]
                if res_key == "Coverage":
                    res_value["description"]["subtitle"] = "Rolling 4 Qtr Pipeline"
                elif res_key == "Qualification":
                    res_value["description"]["subtitle"] = "Rolling 4 Qtr Pipeline in Qualification Stages"
                elif res_key == "Maturity":
                    res_value["description"]["subtitle"] = "Rolling 4 Qtr Pipeline in Mature Stages"
                elif res_key == "Spread":
                    res_value["description"]["subtitle"] = "Reps Pipeline Health"
                else:
                    res_value["description"]["subtitle"] = "Deal Hygiene"
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


# API Endpoint: /table_data
# Method: GET
# Parameters: 'parent_node' (query parameter, string)
# Description: This endpoint fetches child data associated with 'parent_node' id
# Response: A JSON object with data or an error message.
@app.route('/second_chart_data', methods=['GET'])
def get_second_chart_data():
    node = request.args.get('node')
    print(node)
    if not node:
        return jsonify({"status": "Error", "message": "Parent Node parameter is missing"}), 400

    try:
        query = "SELECT * FROM 'health_kpi_full' WHERE node = %s"
        query2 = "SELECT * FROM 'quarterly_figures' WHERE node = %s"
        result = client.query(query, (node, )).result_rows
        result2 = client.query(query2, (node,)).result_rows
        fields = (
            'node', 'as_of_quarter', 'month_label', '4rq_amount', 'qualification_amount', 'maturity_amount',
            '2rq_amount','2rq_mature','qualified_percentage',
            'coverage_status_sum', 'coverage_status_count', 'coverage_percent', '4rq_amount_label', 'maturity_amount_label',
            '2rq_amount_label', '2rq_mature_label','4rq_target',
            'qualification_target', 'maturity_target', '4rq_gap', 'qualification_gap', 'maturity_gap', '2rq_gap',
            '2rq_target', '4rq_target_label', 'qualification_target_label', 'maturity_target_label', '4rq_gap_label', 'qualification_gap_label',
            'maturity_gap_label', '2rq_gap_label', '2rq_target_label', 'calendar_month', '4rq_qq_growth', '4rq_yy_growth',
            'mature_qq_growth', 'mature_yy_growth', 'coverage_factor', 'mature_factor','low_pipe_and_maturity','low_maturity','low_pipe','ideal_pipe_and_maturity',
            'high_risk'
        )

        fields2 = (
            'node', 'cd_rel', 'cd_quarter', 'plan_adj_closed', '4rq_amount', '4rq_gap',
            '4rq_target', 'mature_pipe', 'mature_gap',
            'mature_target', 'title', '4rq_amount_label', '4rq_gap_label',
            '4rq_target_label', 'mature_pipe_label', 'mature_gap_label', 'mature_target_label',
            '4rq_gap_percent'
        )
        response = []
        chart1 = {
            "comments": "1st-chart",
            "header": {
                "title": "4RQ TREND",
                "subtitle": ["GAP"]
            },
            "body": {
                "xValues": [],
                "yValues": [],
                "target": "",
                "unitPrefix": "$",
                "unitSuffix": "M",
            },
            "footer": {
                "Q/Q": "",
                "Y/Y": "",
                "IR": ""
            }
        }

        chart2 = {
            "comments": "2nd-chart",
            "header": {
                "title": "STAGES 2-3 AS % OF 4RQ",
                "subtitle": ["GAP"]
            },
            "body": {
                "xValues": [],
                "yValues": [],
                "target": "",
                "unitSuffix": "%",
                "fillColor": "caribbean-green-60"
            }
        }

        chart3 = {
            "comments": "3rd-chart",
            "header": {
                "title": "MATURITY TREND",
                "subtitle": ["GAP"]
            },
            "body": {
                "xValues": [],
                "yValues": [],
                "target": "",
                "unitPrefix": "$",
                "unitSuffix": "M",
                "fillColor": "midnight-80"
            },
            "footer": {
                "Q/Q": "",
                "Y/Y": "",
                "IR": ""
            }
        }

        chart4 = {
            "comments": "4th-chart",
            "header": {
                "title": "% OF REPS < 2X COVERAGE",
            },
            "body": {
                "xValues": [],
                "yValues": [],
                "target": "",
                "unitSuffix": "%",
                "fillColor": "sunset-red"
            }
        }

        chart5 = {
          "comments": "5th-chart",
          "header": {
            "title": "4RQ PIPELINE BY QTR (GAPS BELOW)"
          },
          "body": {
            "matureValues": [],
            "matureDescriptions": [],
            "thresholdValues": [],
            "unitPrefix": "$",
            "unitSuffix": "M",
            "fillColor": "tropical-sky-20"
          }
        }

        chart6 = {
          "comments": "6th-chart",
          "header": {
            "title": "2 RQW TREND",
            "subtitle": ["GAP", "$270.0M"]
          },
          "body": {
            "xValues": [
            ],
            "yValues": [],
            "target": "",
            "unitPrefix": "$",
            "unitSuffix": "M",
            "fillColor": "sunburst-70"
          }
        }

        chart7 = {
          "comments": "7th-chart",
          "header": {
            "title": "MATURE STAGES (>=5) BY QTR (GAPS BELOW)"
          },
          "body": {
            "matureValues": [],
            "matureDescriptions": [],
            "thresholdValues": [],
            "irPercentValues": [],
            "unitPrefix": "$",
            "unitSuffix": "M",
            "fillColor": ["algae-green-60", "bad-red"]
          },
          "legend": [
            {
              "color": "algae-green-60",
              "text": "Stage 4",
              "value": 0.9
            },
            {
              "color": "bad-red",
              "text": "Stage 5",
              "value": 0.1
            }
          ]
        }

        chart8 = {
          "comments": "8th-chart",
          "header": {
            "title": "COVERAGE QUADRANTS"
          },
          "body": {
            "xValues": [],
            "yValues": []
          },
          "grid": [
            {
              "color": "sunburst-40",
              "textColor": "midnight-grey-80",
              "text": ["Low Pipe &", "Maturity"],
              "value": []
            },
            {
              "color": "midnight-20",
              "textColor": "midnight-grey-80",
              "text": ["Low Pipe"],
              "value": []
            },
            {
              "color": "sunset-red-90",
              "textColor": "white",
              "text": ["High Risk"],
              "value": []
            },
            {
              "color": "aurora-green-20",
              "textColor": "midnight-grey-80",
              "text": ["Ideal Pipe &", "Maturity"],
              "value": []
            },
            {
              "color": "aurora-green-50",
              "textColor": "white",
              "text": ["Low Maturity"],
              "value": []
            }
          ],
          "footer": "4RQ Pipeline Coverage"
        }

        months = []
        for index, val in enumerate(result):
            table_values = dict(zip(fields, val))
            months.append(table_values["calendar_month"])

            if index == 0:
                chart1["body"]["target"] = table_values["4rq_target"]
                chart1["header"]["subtitle"].append(table_values["4rq_gap_label"])
                chart1["footer"]["Q/Q"] = table_values["4rq_qq_growth"]
                chart1["footer"]["Y/Y"] = table_values["4rq_yy_growth"]

                chart2["body"]["target"] = table_values["qualification_target"]
                chart2["header"]["subtitle"].append(table_values["qualification_gap_label"])

                chart3["body"]["target"] = table_values["maturity_target"]
                chart3["header"]["subtitle"].append(table_values["maturity_gap_label"])
                chart3["footer"]["Q/Q"] = table_values["mature_qq_growth"]
                chart3["footer"]["Y/Y"] = table_values["mature_yy_growth"]

                chart6["body"]["target"] = table_values["2rq_target"]
                chart6["header"]["subtitle"].append(table_values["2rq_gap_label"])

                chart8["body"]["xValues"].append(table_values["coverage_factor"])
                chart8["body"]["yValues"].append(table_values["mature_factor"])
                chart8["grid"][0]["value"] = table_values["low_pipe_and_maturity"]
                chart8["grid"][1]["value"] = table_values["low_pipe"]
                chart8["grid"][2]["value"] = table_values["high_risk"]
                chart8["grid"][3]["value"] = table_values["ideal_pipe_and_maturity"]
                chart8["grid"][4]["value"] = table_values["low_maturity"]

            chart1["body"]["yValues"].append(table_values["4rq_amount"])
            chart2["body"]["yValues"].append(table_values["qualified_percentage"])
            chart3["body"]["yValues"].append(table_values["maturity_amount"])
            chart4["body"]["yValues"].append(table_values["coverage_percent"])
            chart6["body"]["yValues"].append(table_values["2rq_amount"])


        dates = [datetime.strptime(date.strip('"'), "%Y.%m.%d") for date in months]

        # Sort the dates
        dates_sorted = sorted(dates)

        # Convert back to the original format with quotes
        sorted_xValues = [f'"{date.strftime("%Y.%m.%d")}"' for date in dates_sorted]

        chart1["body"]["xValues"] = sorted_xValues
        chart2["body"]["xValues"] = sorted_xValues
        chart3["body"]["xValues"] = sorted_xValues
        chart4["body"]["xValues"] = sorted_xValues
        chart6["body"]["xValues"] = sorted_xValues

        for index, val in enumerate(result2):
            table_values2 = dict(zip(fields2, val))
            chart5["body"]["matureValues"].append(table_values2["4rq_gap"])
            chart5["body"]["thresholdValues"].append(table_values2["4rq_target"])
            chart5["body"]["matureDescriptions"].append(table_values2["title"])

            chart7["body"]["matureValues"].append(table_values2["mature_gap"])
            chart7["body"]["thresholdValues"].append(table_values2["mature_target"])
            chart7["body"]["matureDescriptions"].append(table_values2["title"])
            chart7["body"]["irPercentValues"].append(table_values2["4rq_gap_percent"])

        response.append(chart1)
        response.append(chart2)
        response.append(chart3)
        response.append(chart4)
        response.append(chart5)
        response.append(chart6)
        response.append(chart7)
        response.append(chart8)

        return jsonify(response), 200

    except Exception as e:
        return jsonify({"status": "Data not found"}), 404


if __name__ == '__main__':
    app.run()
