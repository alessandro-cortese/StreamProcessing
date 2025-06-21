from pyflink.datastream.functions import MapFunction
from utils.utils import tiff_to_array, now_ms
import datetime
import numpy as np
import logging

logger = logging.getLogger("Q1")
SATURATION_THRESHOLD = 65000

def analyze_saturation(image_array: np.ndarray) -> int:
    if image_array.ndim != 2:
        logger.warning("The image is not 2D, Q1 will ignore the batch.")
        return 0
    saturated_count = np.count_nonzero(image_array > SATURATION_THRESHOLD)
    logger.debug(f"Found {saturated_count} saturated points.")
    return int(saturated_count)

class Q1Saturation(MapFunction):
    def map(self, batch):
        image = tiff_to_array(batch["tif"])
        saturated = analyze_saturation(image)

        batch["image_array"] = image
        batch["saturated"] = saturated
        batch["timestamp"] = datetime.datetime.utcnow().isoformat()
        batch["latency_ms"] = now_ms()
        return batch
