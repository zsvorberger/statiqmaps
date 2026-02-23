# custom_metrics.py

def compute_normalized_speed(activities):
    weighted_speeds = []
    weight_factors = []

    for a in activities:
        if a.get("average_speed") and a.get("elapsed_time"):
            v = a["average_speed"]  # m/s
            t = a["elapsed_time"]   # seconds
            weight = v ** 4
            weighted_speeds.append(t * weight)
            weight_factors.append(weight)

    if weight_factors:
        normalized_speed_mps = sum(weighted_speeds) / sum(weight_factors)
        normalized_speed_kph = round(normalized_speed_mps * 3.6, 2)
    else:
        normalized_speed_kph = "N/A"

    return normalized_speed_kph
