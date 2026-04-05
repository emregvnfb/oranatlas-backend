from datetime import datetime, UTC
from services.coupon_service import generate_daily_coupon_package

def main():
    today = datetime.now(UTC).strftime("%Y-%m-%d")
    package = generate_daily_coupon_package(today)
    print(f"generate_coupons tamamlandı. pool: {len(package.get('pool', []))}")

if __name__ == "__main__":
    main()
