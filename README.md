# OranAtlas Backend Scaffold

Bu paket, senin istediğin dosya yapısına göre hazırlanmış başlangıç backend iskeletidir.

## Kurulum

1. PostgreSQL veritabanı oluştur:
   - db name: `oranatlas`
2. Şemayı yükle:
   - `psql -U postgres -d oranatlas -f sql/schema.sql`
3. Paketleri kur:
   - `pip install -r requirements.txt`
4. `.env` oluştur:
   - `API_FOOTBALL_KEY=...`
   - `DB_HOST=127.0.0.1`
   - `DB_PORT=5432`
   - `DB_NAME=oranatlas`
   - `DB_USER=postgres`
   - `DB_PASSWORD=postgres`
   - `ADMIN_PASSWORD=597212`

## İlk faz komutları

- Maçları çek:
  - `python jobs/update_fixtures.py`
- Oranları çek:
  - `python jobs/collect_odds.py`
- Feature ve prediction üret:
  - `python jobs/build_features.py`
- Günlük kupon üret:
  - `python jobs/generate_coupons.py`
- Sunucuyu aç:
  - `python app.py`

## Mobil uyumlu endpointler

- `/api/health`
- `/api/matches`
- `/api/coupons/today`
- `/api/coupon-results/today`
- `/api/editor-coupon`
- `/api/analyze`
- `/api/admin/action`

Not:
Bu sürüm, tamamen başlangıç iskeleti olarak tasarlandı.
Gerçek model, gelişmiş feature store ve The Odds API historical backfill bir sonraki fazda eklenmeli.


## Python 3.14 notu

Bu sürümde `psycopg2-binary` yerine `psycopg[binary]` kullanıldı.
Böylece Python 3.14 üzerinde Visual C++ Build Tools istemeden kurulum yapılabilir.

## Kurulum kısayolu

1. `.env.example` dosyasını `.env` olarak kopyala
2. `API_FOOTBALL_KEY` alanını doldur
3. `pip install -r requirements.txt`
4. `python jobs/update_fixtures.py`
5. `python jobs/collect_odds.py`
6. `python jobs/build_features.py`
7. `python jobs/generate_coupons.py`
8. `python app.py`
