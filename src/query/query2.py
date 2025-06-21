import numpy as np

# Soglie e configurazioni
EMPTY_THRESHOLD = 5000
SATURATION_THRESHOLD = 65000
OUTLIER_THRESHOLD = 6000
MAX_DIST = 4  # distanza Manhattan massima

# Precalcolo degli offset di Manhattan per vicini interni ed esterni
internal_offsets = [
    (dz, dx, dy)
    for dz in range(3)
    for dx in range(-MAX_DIST, MAX_DIST + 1)
    for dy in range(-MAX_DIST, MAX_DIST + 1)
    if 0 <= abs(dx) + abs(dy) + abs(2 - dz) <= 2
]

external_offsets = [
    (dz, dx, dy)
    for dz in range(3)
    for dx in range(-MAX_DIST, MAX_DIST + 1)
    for dy in range(-MAX_DIST, MAX_DIST + 1)
    if 2 < abs(dx) + abs(dy) + abs(2 - dz) <= MAX_DIST
]


def compute_deviation(image3d, x, y):
    depth, width, height = image3d.shape

    def safe_get(d, i, j):
        if 0 <= d < depth and 0 <= i < width and 0 <= j < height:
            return image3d[d, i, j]
        return None

    close_values = []
    far_values = []

    for dz, dx, dy in internal_offsets:
        val = safe_get(dz, x + dx, y + dy)
        if val is not None and EMPTY_THRESHOLD < val < SATURATION_THRESHOLD:
            close_values.append(val)

    for dz, dx, dy in external_offsets:
        val = safe_get(dz, x + dx, y + dy)
        if val is not None and EMPTY_THRESHOLD < val < SATURATION_THRESHOLD:
            far_values.append(val)

    if not close_values or not far_values:
        return None

    close_avg = np.mean(close_values)
    far_avg = np.mean(far_values)

    return abs(close_avg - far_avg)


def analyze_outliers(image3d):
    """
    Restituisce i 5 punti con la deviazione maggiore dal layer più recente.
    """
    depth, width, height = image3d.shape
    outliers = []

    for x in range(width):
        for y in range(height):
            val = image3d[-1, x, y]
            if val <= EMPTY_THRESHOLD or val >= SATURATION_THRESHOLD:
                continue

            deviation = compute_deviation(image3d, x, y)
            if deviation is not None and deviation > OUTLIER_THRESHOLD:
                outliers.append((x, y, deviation))

    # Ordina per deviazione decrescente
    outliers.sort(key=lambda x: -x[2])
    top5 = outliers[:5]

    result = {}
    for i, (x, y, delta) in enumerate(top5, start=1):
        result[f"P{i}"] = [int(x), int(y)]
        result[f"δP{i}"] = float(delta)

    return result
