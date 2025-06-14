import numpy as np
import logging

logger = logging.getLogger("Q1")

SATURATION_THRESHOLD = 65000

def analyze_saturation(image_array: np.ndarray) -> int:
    """
    Counts the saturated points in an image.
    
    :param image_array: NumPy 2D array of the TIFF image
    :return: number of points with value > SATURATION_THRESHOLD
    """
    if image_array.ndim != 2:
        logger.warning("The image is not 2D, Q1 will ignore the batch.")
        return 0

    saturated_count = np.count_nonzero(image_array > SATURATION_THRESHOLD)
    logger.debug(f"Found {saturated_count} saturated points.")
    return int(saturated_count)
