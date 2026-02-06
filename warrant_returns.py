#!/usr/bin/env python3
"""
Varant Getiri Analizi
Aktif varantların belirtilen tarih aralığındaki getirilerini hesaplar.
"""

import argparse
import warnings
import psycopg2
import pandas as pd
from datetime import date, datetime

# pandas uyarılarını kapat
warnings.filterwarnings('ignore', category=UserWarning)

# Veritabanı bağlantı bilgileri
BARBAR_DSN = "postgresql://postgres:bRmluoDvJJR5caXpIfBTX1gJ7uvcoOIjzHYHqv1gdJ5xxstrFsaoNkPTYjXsJxDI@116.203.73.85/ticks"
FINTABLES_DSN = "postgresql://postgres:h4pjkshmuz9jui06@94.130.134.36:5432/postgres"


def get_active_warrants(end_date, min_expiry=None):
    """
    Belirtilen tarihte aktif olan varantları ve ihraççı bilgilerini fintables_db'den çeker.
    (expiry > end_date olan varantlar)
    min_expiry verilirse, expiry >= min_expiry olan varantları filtreler.
    """
    conn = psycopg2.connect(FINTABLES_DSN)

    if min_expiry:
        query = """
            SELECT
                d.code,
                d.underlying,
                d.option_type,
                d.strike_price,
                d.issuer_id,
                d.expiry,
                COALESCE(b.short_title, b.title, d.issuer_id) as issuer_name
            FROM api_derivative d
            LEFT JOIN api_brokerage b ON d.issuer_id = b.code
            WHERE d.expiry > %s
            AND d.expiry >= %s
            AND d.type = 'warrant'
        """
        df = pd.read_sql(query, conn, params=[str(end_date), str(min_expiry)])
    else:
        query = """
            SELECT
                d.code,
                d.underlying,
                d.option_type,
                d.strike_price,
                d.issuer_id,
                d.expiry,
                COALESCE(b.short_title, b.title, d.issuer_id) as issuer_name
            FROM api_derivative d
            LEFT JOIN api_brokerage b ON d.issuer_id = b.code
            WHERE d.expiry > %s
            AND d.type = 'warrant'
        """
        df = pd.read_sql(query, conn, params=[str(end_date)])

    conn.close()

    # option_type dönüşümü: call -> A, put -> S
    df['option_type'] = df['option_type'].map({'call': 'A', 'put': 'S'}).fillna('-')

    return df


def get_prices(codes, start_date, end_date):
    """Belirtilen varantlar için başlangıç ve bitiş fiyatlarını barbar'dan çeker.
    Sadece start_date ve end_date'te tam olarak verisi olan varantları döndürür.
    """
    conn = psycopg2.connect(BARBAR_DSN)

    # Sadece belirtilen tarihlerde verisi olan varantları al
    # bucket timestamp with timezone olarak saklanıyor, GMT+3 (Europe/Istanbul) olarak çevir
    query = """
        WITH start_prices AS (
            SELECT
                TRIM(code) as code,
                close as start_price,
                (bucket AT TIME ZONE 'Europe/Istanbul') as start_date
            FROM ohlcv_d
            WHERE TRIM(code) = ANY(%s)
            AND (bucket AT TIME ZONE 'Europe/Istanbul')::date = %s::date
        ),
        end_prices AS (
            SELECT
                TRIM(code) as code,
                close as end_price,
                (bucket AT TIME ZONE 'Europe/Istanbul') as end_date
            FROM ohlcv_d
            WHERE TRIM(code) = ANY(%s)
            AND (bucket AT TIME ZONE 'Europe/Istanbul')::date = %s::date
        )
        SELECT
            s.code,
            s.start_price,
            s.start_date,
            e.end_price,
            e.end_date
        FROM start_prices s
        INNER JOIN end_prices e ON s.code = e.code
    """

    df = pd.read_sql(query, conn, params=[list(codes), str(start_date), list(codes), str(end_date)])
    conn.close()
    return df


def calculate_returns(warrants_df, prices_df):
    """Getirileri hesaplar."""
    # Merge işlemi
    merged = warrants_df.merge(prices_df, on='code', how='inner')

    # Getiri hesaplama
    merged['return_pct'] = ((merged['end_price'] - merged['start_price']) / merged['start_price']) * 100

    return merged


def print_results(df, title, ascending=False):
    """Sonuçları konsola yazdırır."""
    print(f"\n{'='*60}")
    print(f" {title}")
    print('='*60)

    sorted_df = df.sort_values('return_pct', ascending=ascending).head(10)

    print(f"{'Kod':<12} {'Dayanak':<8} {'Tip':<4} {'İhraççı':<18} {'Strike':>10} {'Getiri':>12}")
    print('-'*60)

    for _, row in sorted_df.iterrows():
        sign = '+' if row['return_pct'] >= 0 else ''
        print(f"{row['code']:<12} {row['underlying'] or '-':<8} {row['option_type']:<4} "
              f"{(row['issuer_name'] or '-')[:16]:<18} {row['strike_price']:>10.2f} "
              f"{sign}{row['return_pct']:>10.2f}%")


def main():
    parser = argparse.ArgumentParser(description='Varant Getiri Analizi')
    parser.add_argument('--start-date', required=True, help='Başlangıç tarihi (YYYY-MM-DD)')
    parser.add_argument('--end-date', required=True, help='Bitiş tarihi (YYYY-MM-DD)')
    parser.add_argument('--expiry', help='Minimum expiry tarihi (YYYY-MM-DD) - sadece expiry >= bu tarih olan varantlar')
    parser.add_argument('--output', '-o', help='Çıktı dosyası (opsiyonel)')
    parser.add_argument('--format', '-f', choices=['csv', 'json'], default='csv', help='Çıktı formatı (csv veya json)')
    parser.add_argument('--lite', action='store_true', help='Sadece en yüksek 10 ve en düşük 10 varantı kaydet')

    args = parser.parse_args()

    # Tarihleri parse et
    start_date = datetime.strptime(args.start_date, '%Y-%m-%d').date()
    end_date = datetime.strptime(args.end_date, '%Y-%m-%d').date()
    min_expiry = datetime.strptime(args.expiry, '%Y-%m-%d').date() if args.expiry else None

    print(f"Tarih Aralığı: {start_date} - {end_date}")
    if min_expiry:
        print(f"Minimum Expiry: {min_expiry}")
    print("Aktif varantlar yükleniyor...")

    # Aktif varantları çek (bitiş tarihinde henüz expire olmamış olanlar)
    warrants = get_active_warrants(end_date, min_expiry)
    print(f"  {len(warrants)} aktif varant bulundu")

    # Fiyatları çek
    print("Fiyat verileri yükleniyor...")
    prices = get_prices(warrants['code'].tolist(), start_date, end_date)
    print(f"  {len(prices)} varant için fiyat verisi bulundu")

    if prices.empty:
        print("\nBelirtilen tarih aralığında fiyat verisi bulunamadı!")
        return

    # Getirileri hesapla
    print("Getiriler hesaplanıyor...")
    results = calculate_returns(warrants, prices)

    # Sonuçları yazdır
    print_results(results, "EN YÜKSEK GETİRİLİ 10 VARANT", ascending=False)
    print_results(results, "EN DÜŞÜK GETİRİLİ 10 VARANT", ascending=True)

    # Dosyaya kaydet
    if args.output:
        output_cols = ['code', 'underlying', 'option_type', 'issuer_name',
                      'strike_price', 'return_pct', 'start_price', 'end_price',
                      'start_date', 'end_date', 'expiry']

        if args.lite:
            # Sadece en yüksek 10 ve en düşük 10
            top_10 = results.nlargest(10, 'return_pct')
            bottom_10 = results.nsmallest(10, 'return_pct')
            output_data = pd.concat([top_10, bottom_10])[output_cols]
            row_count = 20
        else:
            output_data = results[output_cols].sort_values('return_pct', ascending=False)
            row_count = len(output_data)

        if args.format == 'json':
            # JSON formatında kaydet
            output_data['start_date'] = output_data['start_date'].astype(str)
            output_data['end_date'] = output_data['end_date'].astype(str)
            output_data['expiry'] = output_data['expiry'].astype(str)
            output_data.to_json(args.output, orient='records', force_ascii=False, indent=2)
            print(f"\nSonuçlar kaydedildi (JSON{' - lite' if args.lite else ''} - {row_count} satır): {args.output}")
        else:
            # CSV formatında kaydet
            output_data.to_csv(args.output, index=False, encoding='utf-8-sig')
            print(f"\nSonuçlar kaydedildi (CSV{' - lite' if args.lite else ''} - {row_count} satır): {args.output}")

    print(f"\nToplam analiz edilen varant: {len(results)}")


if __name__ == '__main__':
    main()
