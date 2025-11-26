import os
import logging
from datetime import datetime

def setup_logging(name=None):
    """
    Configure logging for the review process
    
    Args:
        name (str, optional): Logger name. If None, uses __name__
        
    Returns:
        logging.Logger: Configured logger instance
    """
    # Create logs directory if it doesn't exist
    log_dir = os.path.join(os.getcwd(), 'logs')
    os.makedirs(log_dir, exist_ok=True)
    
    # Create log file name with timestamp
    log_file = os.path.join(log_dir, f'review_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log')
    
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler()  # This will also print to console
        ]
    )
    
    return logging.getLogger(name or __name__)