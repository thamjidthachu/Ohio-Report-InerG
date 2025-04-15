from flask import Flask, request, jsonify
import sqlite3

app = Flask(__name__)


@app.route('/data', methods=['GET'])
def get_well_data():
    well_id = request.args.get('well')
    if not well_id:
        return jsonify({"error": "Missing 'well' parameter"}), 400

    conn = sqlite3.connect('production.db')
    cursor = conn.cursor()

    cursor.execute("""
        SELECT oil, gas, brine
        FROM annual_production
        WHERE api_well_number = ?
    """, (well_id,))
    result = cursor.fetchone()

    conn.close()

    if result:
        oil, gas, brine = result
        return jsonify({
            "oil": oil,
            "gas": gas,
            "brine": brine
        })
    else:
        return jsonify({"error": "Well not found"}), 404


if __name__ == '__main__':
    import process_data
    process_data.process_data()

    app.run(port=8080)
