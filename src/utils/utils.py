import numpy as np
from PIL import Image
import io
import logging
import time

logger = logging.getLogger("Utils")

def tiff_to_array(tiff_bytes: bytes) -> np.ndarray:
    """
    Converts a binary TIFF image into a NumPy array.
    
    :param tiff_bytes: binary TIFF data from the challenger
    :return: 2D NumPy array of the image
    """
    try:
        image = Image.open(io.BytesIO(tiff_bytes))
        image_array = np.array(image)
        return image_array
    except Exception as e:
        logger.error(f"Error during TIFF conversion: {e}")
        return np.zeros((1, 1))                                     # secure fallback

def now_ms() -> int:
    """Returns the current time in milliseconds."""
    return int(time.time() * 1000)
