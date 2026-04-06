# ORANATLAS - COUPON SERVICE V33 FINAL FIX

def build_coupon(pool, target_size):
    selected = []
    used_matches = set()

    # Ana seçim (zorunlu doldurma mantığı)
    for item in pool:
        if len(selected) >= target_size:
            break

        if item.get("fixture_id") in used_matches:
            continue

        selected.append(item)
        used_matches.add(item.get("fixture_id"))

    # Fallback: eksikse doldur
    if len(selected) < target_size:
        for item in pool:
            if len(selected) >= target_size:
                break

            if item not in selected:
                selected.append(item)

    return selected


def generate_coupons(pool):
    return {
        "coupons_4": [{
            "coupon_size": 4,
            "items": build_coupon(pool, 4)
        }],
        "coupons_5": [{
            "coupon_size": 5,
            "items": build_coupon(pool, 5)
        }],
        "coupons_6": [{
            "coupon_size": 6,
            "items": build_coupon(pool, 6)
        }]
    }
