from flask import Flask, render_template, request, jsonify
import os
import traceback
from Review.review_logic import run_review
from datetime import datetime
from config import get_index_config

app = Flask(__name__)

@app.route('/')
def home():
    return render_template('index.html')

@app.route('/calculate', methods=['POST'])
def calculate():
    try:
        # Get form data
        review_type = request.form.get('review_type', 'FRI4P')
        date = request.form['date']
        effective_date = request.form['effective_date']
        currency = request.form.get('currency', 'EUR')

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
        if result["status"] == "success":
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