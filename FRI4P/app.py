# app.py
from flask import Flask, render_template, request, jsonify
import os
from Review.review_logic import run_review
from datetime import datetime

app = Flask(__name__)

@app.route('/')
def home():
    return render_template('index.html')

@app.route('/calculate', methods=['POST'])
def calculate():
    try:
        # Get form data
        date = request.form['date']
        effective_date = request.form['effective_date']
        index = request.form.get('index', 'FRI4P')
        isin = request.form.get('isin', 'FRIX00003643')
        currency = request.form.get('currency', 'EUR')

        # Run the review calculation
        result = run_review(
            date=date,
            effective_date=effective_date,
            index=index,
            isin=isin,
            currency=currency
        )

        # Open Excel file if calculation was successful
        if result["status"] == "success":
            if os.path.exists(result["data"]["fri4p_path"]):
                os.startfile(result["data"]["fri4p_path"])

        return jsonify(result)

    except Exception as e:
        return jsonify({
            "status": "error",
            "message": str(e)
        }), 500

if __name__ == '__main__':
    app.run(debug=True, port=5000)