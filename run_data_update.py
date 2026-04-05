import subprocess
import sys
import os
from pathlib import Path
from datetime import datetime

PROJECT_ROOT = Path(__file__).resolve().parent

def run_step(name, module_name):
    print(f"\n🚀 {name} başlatılıyor...")
    try:
        env = os.environ.copy()
        env["PYTHONPATH"] = str(PROJECT_ROOT) + os.pathsep + env.get("PYTHONPATH", "")
        subprocess.run(
            [sys.executable, "-m", module_name],
            cwd=str(PROJECT_ROOT),
            env=env,
            check=True,
        )
        print(f"✅ {name} tamamlandı")
    except subprocess.CalledProcessError as e:
        print(f"❌ {name} hata verdi: {e}")
    except Exception as e:
        print(f"❌ {name} beklenmeyen hata: {e}")

def main():
    print("=" * 40)
    print("🚀 ORANATLAS DATA UPDATE BAŞLADI")
    print(f"⏱️ Zaman: {datetime.now()}")
    print(f"📁 Proje klasörü: {PROJECT_ROOT}")
    print(f"🐍 Python: {sys.executable}")
    print("=" * 40)

    run_step("Maçları çekme", "jobs.update_fixtures")
    run_step("Oranları çekme", "jobs.collect_odds")
    run_step("Feature + AI üretimi", "jobs.build_features")

    print("\n" + "=" * 40)
    print("🎯 DATA UPDATE TAMAMLANDI")
    print("=" * 40)

if __name__ == "__main__":
    main()
