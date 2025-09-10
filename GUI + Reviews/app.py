from flask import Flask, render_template, request, jsonify
import os
import socket
import traceback
from datetime import datetime
from config import get_index_config
from task_manager import task_manager

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
        """Start a single review task in background"""
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
            
            # Check if request is from local machine (for auto-open)
            user_ip = request.environ.get('HTTP_X_FORWARDED_FOR', request.environ.get('REMOTE_ADDR'))
            try:
                server_ip = socket.gethostbyname(socket.gethostname())
            except:
                server_ip = None
            is_local_request = user_ip in ['127.0.0.1', '::1', 'localhost'] or user_ip == server_ip

            # Create background task
            task_id = task_manager.create_task(
                task_type="single",
                review_type=review_type,
                date=date,
                co_date=co_date,
                effective_date=effective_date,
                index=index_config["index"],
                isin=index_config["isin"],
                currency=currency,
                auto_open=auto_open,
                is_local_request=is_local_request
            )
            
            # Start the task
            task_manager.start_single_review_task(task_id)
            
            return jsonify({
                "status": "started",
                "message": f"Review task started successfully",
                "task_id": task_id,
                "estimated_duration": "This may take several minutes to complete"
            })

        except Exception as e:
            print(f"Error in calculate route: {str(e)}")
            print(traceback.format_exc())
            return jsonify({
                "status": "error",
                "message": str(e)
            }), 500

    @app.route('/calculate-batch', methods=['POST'])
    def calculate_batch():
        """Start a batch review task in background"""
        try:
            # Get form data
            review_types = request.form.getlist('review_types[]')
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
            
            # Check if request is from local machine (for auto-open)
            user_ip = request.environ.get('HTTP_X_FORWARDED_FOR', request.environ.get('REMOTE_ADDR'))
            try:
                server_ip = socket.gethostbyname(socket.gethostname())
            except:
                server_ip = None
            is_local_request = user_ip in ['127.0.0.1', '::1', 'localhost'] or user_ip == server_ip

            # Create background task
            task_id = task_manager.create_task(
                task_type="batch",
                review_types=review_types,
                date=date,
                co_date=co_date,
                effective_date=effective_date,
                currency=currency,
                auto_open=auto_open,
                is_local_request=is_local_request
            )
            
            # Start the task
            task_manager.start_batch_review_task(task_id)

            return jsonify({
                "status": "started",
                "message": f"Batch review task started for {len(review_types)} reviews",
                "task_id": task_id,
                "estimated_duration": f"This may take {len(review_types) * 2}-{len(review_types) * 5} minutes to complete"
            })

        except Exception as e:
            print(f"Error in calculate-batch route: {str(e)}")
            print(traceback.format_exc())
            return jsonify({
                "status": "error",
                "message": str(e)
            }), 500

    @app.route('/task-status/<task_id>', methods=['GET'])
    def get_task_status(task_id):
        """Get the current status of a task"""
        try:
            task_status = task_manager.get_task_status_dict(task_id)
            
            if task_status is None:
                return jsonify({
                    "status": "error",
                    "message": "Task not found"
                }), 404
            
            return jsonify({
                "status": "success",
                "task": task_status
            })
            
        except Exception as e:
            return jsonify({
                "status": "error",
                "message": str(e)
            }), 500

    @app.route('/cancel-task/<string:task_id>', methods=['POST'])
    def cancel_task(task_id):
        """Cancel a pending task"""
        try:
            success = task_manager.cancel_task(task_id)
            
            if success:
                return jsonify({
                    "status": "success",
                    "message": "Task cancelled successfully"
                })
            else:
                return jsonify({
                    "status": "error",
                    "message": "Task could not be cancelled (may be running or completed)"
                })
                
        except Exception as e:
            return jsonify({
                "status": "error",
                "message": str(e)
            }), 500

    @app.route('/cleanup-tasks', methods=['POST'])
    def cleanup_old_tasks():
        """Clean up old completed tasks"""
        try:
            max_age_hours = request.json.get('max_age_hours', 24) if request.is_json else 24
            removed_count = task_manager.cleanup_old_tasks(max_age_hours)
            
            return jsonify({
                "status": "success",
                "message": f"Cleaned up {removed_count} old tasks"
            })
            
        except Exception as e:
            return jsonify({
                "status": "error",
                "message": str(e)
            }), 500

    @app.route('/system-status', methods=['GET'])
    def get_system_status():
        """Get overall system status"""
        try:
            running_tasks = task_manager.get_running_tasks_count()
            can_start_new = task_manager.can_start_new_task()
            
            return jsonify({
                "status": "success",
                "system": {
                    "running_tasks": running_tasks,
                    "max_concurrent_tasks": task_manager.max_concurrent_tasks,
                    "can_start_new_task": can_start_new,
                    "total_tasks": len(task_manager.tasks)
                }
            })
            
        except Exception as e:
            return jsonify({
                "status": "error",
                "message": str(e)
            }), 500
        
    return app

if __name__ == '__main__':
    app = create_app()
    # Keep auto-reload enabled for development
    app.run(host='0.0.0.0', debug=True, port=5000, use_reloader=True, threaded=True)