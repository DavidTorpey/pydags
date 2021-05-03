"""
This initialisation script it called the first time pydags is imported.
Currently, the only purpose of the script is to set up the root logger.
"""

import logging

logging.basicConfig(
    format='pydags: %(name)s - %(levelname)s - %(message)s', level=logging.INFO
)
