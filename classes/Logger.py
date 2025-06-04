import logging

# Configure logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(name)s:%(lineno)d - %(message)s'
)

logger = logging.getLogger(__name__)