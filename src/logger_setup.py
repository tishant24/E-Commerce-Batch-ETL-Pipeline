import logging
import os
from datetime import datetime
from src.config import PathConfig


def get_logger(name="ETLPipeline"):
    os.makedirs(PathConfig.LOG_DIR, exist_ok=True)

    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)

    if logger.handlers:
        return logger

    formatter = logging.Formatter(
        fmt="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )

    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(formatter)

    file_handler = logging.FileHandler(PathConfig.LOG_FILE, mode="a", encoding="utf-8")
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(formatter)

    logger.addHandler(console_handler)
    logger.addHandler(file_handler)

    return logger


class PipelineTracker:
    def __init__(self, logger):
        self.logger = logger
        self.job_start = None
        self.job_end = None
        self.metrics = {}

    def start(self):
        self.job_start = datetime.now()
        self.logger.info("=" * 70)
        self.logger.info("PIPELINE STARTED")
        self.logger.info(f"Job Start Time : {self.job_start.strftime('%Y-%m-%d %H:%M:%S')}")
        self.logger.info("=" * 70)

    def log_stage(self, stage_name, record_count=None, extra=None):
        msg = f"STAGE [{stage_name}]"
        if record_count is not None:
            msg += f" | Records: {record_count:,}"
        if extra:
            msg += f" | {extra}"
        self.logger.info(msg)

    def log_metric(self, key, value):
        self.metrics[key] = value
        self.logger.debug(f"Metric recorded -> {key}: {value}")

    def finish(self, validation_status="PASSED"):
        self.job_end = datetime.now()
        duration = (self.job_end - self.job_start).total_seconds()
        self.logger.info("=" * 70)
        self.logger.info("PIPELINE FINISHED")
        self.logger.info(f"Job End Time       : {self.job_end.strftime('%Y-%m-%d %H:%M:%S')}")
        self.logger.info(f"Total Duration     : {duration:.2f} seconds")
        self.logger.info(f"Validation Status  : {validation_status}")
        for key, val in self.metrics.items():
            self.logger.info(f"  {key:<30}: {val}")
        self.logger.info("=" * 70)
