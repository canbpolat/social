#!/usr/bin/env python3
"""
Varant Getiri Analizi - Web Arayüzü
Flask uygulaması: Varant verilerini görselleştirme ve PNG indirme
"""

from flask import Flask, render_template, request, jsonify
import pandas as pd
from datetime import datetime, date

# Mevcut warrant_returns.py'den fonksiyonları import et
from warrant_returns import (
    get_active_warrants, get_prices, calculate_returns,
    load_warrants_from_csv, load_prices_from_csv
)

app = Flask(__name__)

# Türkçe ay isimleri
TURKISH_MONTHS = {
    1: 'Ocak', 2: 'Şubat', 3: 'Mart', 4: 'Nisan',
    5: 'Mayıs', 6: 'Haziran', 7: 'Temmuz', 8: 'Ağustos',
    9: 'Eylül', 10: 'Ekim', 11: 'Kasım', 12: 'Aralık'
}

def format_date_turkish(d):
    """Tarihi Türkçe formatta döndürür: 29 Ocak 2026"""
    return f"{d.day} {TURKISH_MONTHS[d.month]} {d.year}"


def format_date_range(start, end):
    """Tarih aralığını kısa formatta döndürür:
    Aynı ay/yıl: 23 - 30 Ocak 2026
    Farklı ay, aynı yıl: 23 Ocak - 30 Şubat 2026
    Farklı yıl: 23 Ocak 2026 - 30 Şubat 2027
    """
    if start.year == end.year and start.month == end.month:
        return f"{start.day} - {end.day} {TURKISH_MONTHS[end.month]} {end.year}"
    elif start.year == end.year:
        return f"{start.day} {TURKISH_MONTHS[start.month]} - {end.day} {TURKISH_MONTHS[end.month]} {end.year}"
    else:
        return f"{format_date_turkish(start)} - {format_date_turkish(end)}"


@app.route('/')
def index():
    """Ana sayfa - Tarih seçim formu"""
    # Varsayılan tarihler: son 1 hafta
    today = date.today()
    default_end = today.strftime('%Y-%m-%d')
    default_start = (today.replace(day=today.day - 7) if today.day > 7
                     else today.replace(month=today.month - 1, day=28)).strftime('%Y-%m-%d')

    return render_template('index.html',
                         default_start=default_start,
                         default_end=default_end,
                         default_expiry=default_end)


@app.route('/analyze', methods=['POST'])
def analyze():
    """Varant analizi yap ve sonuçları göster"""
    try:
        # Form'dan tarihleri al
        start_date = request.form.get('start_date')
        end_date = request.form.get('end_date')
        expiry_date = request.form.get('expiry_date')

        # Tarihleri parse et
        start = datetime.strptime(start_date, '%Y-%m-%d').date()
        end = datetime.strptime(end_date, '%Y-%m-%d').date()
        expiry = datetime.strptime(expiry_date, '%Y-%m-%d').date() if expiry_date else None

        # CSV dosyaları kontrol et
        warrants_file = request.files.get('warrants_csv')
        prices_file = request.files.get('prices_csv')

        has_warrants_csv = warrants_file and warrants_file.filename != ''
        has_prices_csv = prices_file and prices_file.filename != ''

        if has_warrants_csv != has_prices_csv:
            return render_template('results.html',
                                 error="Her iki CSV dosyası da yüklenmelidir veya hiçbiri yüklenmeyin.",
                                 start_date=start_date,
                                 end_date=end_date)

        if has_warrants_csv and has_prices_csv:
            # CSV modu
            warrants = load_warrants_from_csv(warrants_file, end, expiry)
            prices = load_prices_from_csv(prices_file, warrants['code'].tolist(), start, end)
            data_source = 'CSV'
        else:
            # DB modu
            warrants = get_active_warrants(end, expiry)
            prices = get_prices(warrants['code'].tolist(), start, end)
            data_source = 'Veritabanı'

        if prices.empty:
            return render_template('results.html',
                                 error="Belirtilen tarih aralığında veri bulunamadı!",
                                 start_date=start_date,
                                 end_date=end_date)

        # Getirileri hesapla
        results = calculate_returns(warrants, prices)

        # En yüksek ve en düşük 10 varantı ayır
        top_10 = results.nlargest(10, 'return_pct')
        bottom_10 = results.nsmallest(10, 'return_pct')

        # DataFrame'leri dict listesine çevir (HTML için)
        rising_data = top_10.to_dict('records')
        falling_data = bottom_10.to_dict('records')

        # Tarih aralığını Türkçe formatla
        date_range = format_date_range(start, end)

        return render_template('results.html',
                             rising_warrants=rising_data,
                             falling_warrants=falling_data,
                             date_range=date_range,
                             start_date=start_date,
                             end_date=end_date,
                             total_analyzed=len(results),
                             data_source=data_source)

    except Exception as e:
        return render_template('results.html',
                             error=f"Hata oluştu: {str(e)}",
                             start_date=start_date if 'start_date' in dir() else '',
                             end_date=end_date if 'end_date' in dir() else '')


if __name__ == '__main__':
    print("Varant Getiri Analizi Web Arayüzü")
    print("Tarayıcıda açın: http://localhost:5000")
    app.run(debug=True, host='0.0.0.0', port=5001)
