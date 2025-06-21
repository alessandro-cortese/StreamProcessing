import numpy as np
import tifffile
import time

def tiff_to_array(tif_bytes: bytes) -> np.ndarray:
    return tifffile.imread(tif_bytes)

def now_ms() -> int:
    return int(time.time() * 1000)

