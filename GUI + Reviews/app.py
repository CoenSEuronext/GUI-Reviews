from flask import Flask, render_template, request, jsonify
import os
import socket
import traceback
from Review.review_logic import run_review
from datetime import datetime
from config import get_index_config
from batch_processor import run_batch_reviews

# Initialize configurations once at startup
try:
    from config import INDEX_CONFIGS
    frontend_configs = {
        key: {
            'index': value['index'],
            'isin': value['isin']
        }
        for key, value in INDEX_CONFIGS.items()
    }
except Exception as e:
    print(f"Error initializing configs: {str(e)}")
    frontend_configs = {}

def create_app():
    app = Flask(__name__)
    
    # Add configuration
    app.config['SEND_FILE_MAX_AGE_DEFAULT'] = 0
    app.config['TEMPLATES_AUTO_RELOAD'] = True

    @app.route('/')
    def home():
        return render_template('index.html', INDEX_CONFIGS=frontend_configs)

    @app.route('/calculate', methods=['POST'])
    def calculate():
        try:
            # Get form data
            review_type = request.form['review_type']
            date = request.form['date']
            co_date = request.form['co_date']
            effective_date = request.form['effective_date']
            currency = request.form['currency']
            auto_open = request.form.get('auto_open') == 'on'
            
            try:
                effective_date = datetime.strptime(effective_date, "%d-%b-%y").strftime("%d-%b-%y")
            except ValueError as ve:
                raise ValueError("Effective Date must be in the format 'DD-MMM-YY'. Please correct the input.")

            # Get index configuration
            index_config = get_index_config(review_type)

            # Run the review calculation
            result = run_review(
                review_type=review_type,
                date=date,
                co_date=co_date,
                effective_date=effective_date,
                index=index_config["index"],
                isin=index_config["isin"],
                currency=currency
            )

            # Open Excel file if calculation was successful and request is from local machine
            if result["status"] == "success" and auto_open:
                # Get the user's IP address
                user_ip = request.environ.get('HTTP_X_FORWARDED_FOR', request.environ.get('REMOTE_ADDR'))
                
                # Get server's local IP
                try:
                    server_ip = socket.gethostbyname(socket.gethostname())
                except:
                    server_ip = None
                
                # Only auto-open if the request is from the same machine
                is_local_request = user_ip in ['127.0.0.1', '::1', 'localhost'] or user_ip == server_ip
                
                if is_local_request:
                    output_path = result["data"].get(index_config["output_key"])
                    if output_path and os.path.exists(output_path):
                        os.startfile(output_path)
                        print(f"File opened automatically for local user: {user_ip}")
                else:
                    print(f"Auto-open skipped for remote user: {user_ip} (server IP: {server_ip})")

            return jsonify(result)

        except Exception as e:
            print(f"Error in calculate route: {str(e)}")
            print(traceback.format_exc())
            return jsonify({
                "status": "error",
                "message": str(e)
            }), 500
    @app.route('/calculate-batch', methods=['POST'])
    def calculate_batch():
        """Handle multiple review calculations"""
        try:
            # Get form data
            review_types = request.form.getlist('review_types[]')  # List of selected review types
            date = request.form['date']
            co_date = request.form['co_date']
            effective_date = request.form['effective_date']
            currency = request.form['currency']
            auto_open = request.form.get('auto_open') == 'on'
            
            if not review_types:
                return jsonify({
                    "status": "error",
                    "message": "No review types selected"
                }), 400

            # Validate effective date format
            try:
                effective_date = datetime.strptime(effective_date, "%d-%b-%y").strftime("%d-%b-%y")
            except ValueError as ve:
                raise ValueError("Effective Date must be in the format 'DD-MMM-YY'. Please correct the input.")

            # Run batch reviews
              # New module we'll create
            
            results = run_batch_reviews(
                review_types=review_types,
                date=date,
                co_date=co_date,
                effective_date=effective_date,
                currency=currency,
                auto_open=auto_open
            )

            return jsonify({
                "status": "success",
                "message": f"Completed {len(results)} reviews",
                "results": results
            })

        except Exception as e:
            print(f"Error in calculate-batch route: {str(e)}")
            print(traceback.format_exc())
            return jsonify({
                "status": "error",
                "message": str(e)
            }), 500
        
    return app

if __name__ == '__main__':
    app = create_app()
    app.run(host='0.0.0.0', debug=True, port=5000, use_reloader=True, threaded=True)