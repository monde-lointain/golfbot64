from bot import run_bot

import logging
import sys
from logging.handlers import TimedRotatingFileHandler

# Configure root logger (same as before)
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Create a handler for log file
handler = TimedRotatingFileHandler("private/logs/bot_logs.log", when="midnight", interval=1, backupCount=30)
handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s"))
logger.addHandler(handler)

# Capture sys.stderr and write to both the terminal and the log file
class TerminalAndLogFile:
    def __init__(self, *file_objects):
        self.file_objects = file_objects

    def write(self, message):
        for file_obj in self.file_objects:
            file_obj.write(message)
            file_obj.flush()  # Flush the buffer to ensure immediate writing

sys.stderr = TerminalAndLogFile(sys.stderr, handler.stream)  # Redirect stderr to log file

if __name__ == "__main__":
    run_bot()
