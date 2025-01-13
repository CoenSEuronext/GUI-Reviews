from flask import Flask, render_template, request, jsonify
import os
import traceback
from Review.review_logic import run_review
from datetime import datetime
from config import get_index_config

app = Flask(__name__)

@app.route('/')
def home():
    from config import INDEX_CONFIGS
    # Remove output_key as it's not needed in frontend
    frontend_configs = {
        key: {
            'index': value['index'],
            'isin': value['isin']
        }
        for key, value in INDEX_CONFIGS.items()
    }
    return render_template('index.html', INDEX_CONFIGS=frontend_configs)

@app.route('/calculate', methods=['POST'])
def calculate():
    try:
        # Get form data
        review_type = request.form['review_type']
        date = request.form['date']
        effective_date = request.form['effective_date']
        currency = request.form['currency']
        auto_open = request.form.get('auto_open') == 'on'

        # Get index configuration
        index_config = get_index_config(review_type)

        # Run the review calculation
        result = run_review(
            review_type=review_type,
            date=date,
            effective_date=effective_date,
            index=index_config["index"],
            isin=index_config["isin"],
            currency=currency
        )

        # Open Excel file if calculation was successful
        if result["status"] == "success" and auto_open:
            output_path = result["data"].get(index_config["output_key"])
            if output_path and os.path.exists(output_path):
                os.startfile(output_path)

        return jsonify(result)

    except Exception as e:
        print(f"Error in calculate route: {str(e)}")
        print(traceback.format_exc())
        return jsonify({
            "status": "error",
            "message": str(e)
        }), 500

if __name__ == '__main__':
    app.run(debug=True, port=5000)