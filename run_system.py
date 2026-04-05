import subprocess
import datetime
import sys
import os
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent

def run_command(module_name, name):
    print(f"\n🚀 {name} başlatılıyor...")
    try:
        env = os.environ.copy()
        env["PYTHONPATH"] = str(PROJECT_ROOT) + os.pathsep + env.get("PYTHONPATH", "")
        subprocess.run(
            [sys.executable, "-m", module_name],
            cwd=str(PROJECT_ROOT),
            env=env,
            check=True
        )
        print(f"✅ {name} tamamlandı")
    except subprocess.CalledProcessError as e:
        print(f"❌ {name} hata verdi:", e)

def run_all():
    print("====================================")
    print("🚀 ORANATLAS FULL SİSTEM BAŞLADI")
    print("⏱️ Zaman:", datetime.datetime.now())
    print("📁 Proje klasörü:", PROJECT_ROOT)
    print("🐍 Python:", sys.executable)
    print("====================================")

    # 1. Fixtures çek
    run_command("jobs.update_fixtures", "Maçları çekme")

    # 2. Odds çek
    run_command("jobs.collect_odds", "Oranları çekme")

    # 3. Feature + AI
    run_command("jobs.build_features", "Feature + AI üretimi")

    # 4. Kupon üret
    run_command("jobs.generate_coupons", "Kupon üretimi")

    print("\n📊 Sonuçlar API üzerinden kontrol edilecek")

    print("\n====================================")
    print("🎯 TÜM SİSTEM TAMAMLANDI")
    print("====================================")


if __name__ == "__main__":
    run_all()
