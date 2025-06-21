from pyflink.datastream.functions import KeyedProcessFunction, RuntimeContext
import numpy as np

EMPTY_THRESHOLD = 5000
SATURATION_THRESHOLD = 65000
OUTLIER_THRESHOLD = 6000
MAX_DIST = 4

internal_offsets = np.array([
    (dz, dx, dy)
    for dz in range(3)
    for dx in range(-MAX_DIST, MAX_DIST + 1)
    for dy in range(-MAX_DIST, MAX_DIST + 1)
    if 0 <= abs(dx) + abs(dy) + abs(2 - dz) <= 2
], dtype=np.int32)

external_offsets = np.array([
    (dz, dx, dy)
    for dz in range(3)
    for dx in range(-MAX_DIST, MAX_DIST + 1)
    for dy in range(-MAX_DIST, MAX_DIST + 1)
    if 2 < abs(dx) + abs(dy) + abs(2 - dz) <= MAX_DIST
], dtype=np.int32)

class Q2SlidingWindow(KeyedProcessFunction):
    def open(self, runtime_context: RuntimeContext):
        self.state = runtime_context.get_state(
            name="window_state",
            state_type=None 
        )

    def process_element(self, batch, ctx):
        image = batch["image_array"]
        window = self.state.value() or []
        window.append(image)
        if len(window) > 3:
            window.pop(0)
        self.state.update(window)

        if len(window) == 3:
            image3d = np.stack(window, axis=0)
            outliers = self.analyze_outliers(image3d)
            batch["q2_outliers"] = outliers
        return [batch]

    def analyze_outliers(self, image3d):
        padded = np.pad(image3d, ((0, 0), (MAX_DIST, MAX_DIST), (MAX_DIST, MAX_DIST)), constant_values=0)
        outliers = []
        _, width, height = image3d.shape

        for x in range(width):
            for y in range(height):
                val = padded[2, x + MAX_DIST, y + MAX_DIST]
                if val <= EMPTY_THRESHOLD or val >= SATURATION_THRESHOLD:
                    continue

                close_vals = []
                for dz, dx, dy in internal_offsets:
                    v = padded[dz, x + dx + MAX_DIST, y + dy + MAX_DIST]
                    if EMPTY_THRESHOLD < v < SATURATION_THRESHOLD:
                        close_vals.append(v)

                far_vals = []
                for dz, dx, dy in external_offsets:
                    v = padded[dz, x + dx + MAX_DIST, y + dy + MAX_DIST]
                    if EMPTY_THRESHOLD < v < SATURATION_THRESHOLD:
                        far_vals.append(v)

                if not close_vals or not far_vals:
                    continue

                dev = abs(np.mean(close_vals) - np.mean(far_vals))
                if dev > OUTLIER_THRESHOLD:
                    outliers.append((x, y, dev))

        outliers.sort(key=lambda x: -x[2])
        top5 = outliers[:5]

        result = {}
        for i, (x, y, delta) in enumerate(top5, start=1):
            result[f"P{i}"] = [int(x), int(y)]
            result[f"δP{i}"] = float(delta)
        return result
