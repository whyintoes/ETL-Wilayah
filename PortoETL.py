# ==============================================================================
# 1. IMPORT LIBRARY
# ==============================================================================
import requests
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account
import time # Kita tambahkan library time untuk memberi jeda

# ==============================================================================
# 2. KONFIGURASI
# ==============================================================================
# --- Konfigurasi API ---
# 🔑 Ganti dengan API Key Anda yang valid dan sudah subscribe ke API Wilayah
API_KEY = "34edec7d-e4ba-596f-966b-6bb02f2d"
# Base URL untuk endpoint Wilayah
URL_BASE = "https://api.goapi.io/regional"

# --- Konfigurasi Google BigQuery ---
KEY_PATH = "/Users/wahyues/DS PWDK/Porto ETL/project-api-wilayah-1414-c5882d43ac99.json"
PROJECT_ID = "project-api-wilayah-1414"
# 📊 Menggunakan dataset 'dataset_wilayah' yang baru dan relevan
TABLE_ID = f"{PROJECT_ID}.dataset_wilayah.master_wilayah_indonesia"

# ==============================================================================
# 3. FUNGSI UNTUK MENGAMBIL DATA DARI API WILAYAH
# ==============================================================================
def fetch_wilayah_data(api_key, url_base):
    """
    Mengambil seluruh data wilayah (provinsi, kota, kecamatan) dari GoAPI.
    """
    semua_wilayah = []
    print("🚀 Memulai pengambilan data seluruh wilayah Indonesia...")

    try:
        # --- Langkah 1: Ambil semua provinsi ---
        url_provinsi = f"{url_base}/provinsi"
        params_provinsi = {'api_key': api_key}
        response_provinsi = requests.get(url_provinsi, params=params_provinsi)
        response_provinsi.raise_for_status()
        provinsi_list = response_provinsi.json()['data']
        print(f"✅ Ditemukan {len(provinsi_list)} provinsi.")

        # --- Langkah 2: Looping setiap provinsi untuk mengambil kota/kabupaten ---
        for prov in provinsi_list:
            id_prov = prov['id']
            nama_prov = prov['name']
            print(f"\nMengambil data untuk provinsi: {nama_prov}...")
            
            url_kota = f"{url_base}/kota"
            params_kota = {'api_key': api_key, 'provinsi_id': id_prov}
            response_kota = requests.get(url_kota, params=params_kota)
            kota_list = response_kota.json()['data']
            print(f"  > Ditemukan {len(kota_list)} kota/kabupaten.")
            time.sleep(0.5)

            # --- Langkah 3: Looping setiap kota untuk mengambil kecamatan ---
            for kota in kota_list:
                id_kota = kota['id']
                nama_kota = kota['name']
                
                url_kecamatan = f"{url_base}/kecamatan"
                params_kecamatan = {'api_key': api_key, 'kota_id': id_kota}
                response_kecamatan = requests.get(url_kecamatan, params=params_kecamatan)
                
                # --- PERBAIKAN DIMULAI DI SINI ---
                kecamatan_json = response_kecamatan.json()
                
                # Periksa apakah kunci 'data' ada di dalam respons
                if 'data' in kecamatan_json and kecamatan_json['data']:
                    kecamatan_list = kecamatan_json['data']
                    print(f"    - Kota '{nama_kota}' memiliki {len(kecamatan_list)} kecamatan.")
                    
                    # --- Langkah 4: Simpan data kecamatan beserta induknya ---
                    for kec in kecamatan_list:
                        data_rapi = {
                            'id_provinsi': id_prov,
                            'nama_provinsi': nama_prov,
                            'id_kota': id_kota,
                            'nama_kota': nama_kota,
                            'id_kecamatan': kec['id'],
                            'nama_kecamatan': kec['name']
                        }
                        semua_wilayah.append(data_rapi)
                else:
                    # Jika kunci 'data' tidak ada, cetak pesan dan lanjutkan
                    print(f"    - Kota '{nama_kota}' tidak memiliki data kecamatan yang valid. Respons: {kecamatan_json}. Melanjutkan...")
                # --- PERBAIKAN SELESAI ---
                
                time.sleep(0.5)

    except requests.exceptions.RequestException as e:
        print(f"❌ Terjadi error saat request ke API: {e}")
        return None

    if semua_wilayah:
        return pd.DataFrame(semua_wilayah)
    else:
        return None
# ==============================================================================
# 4. FUNGSI UNTUK MENGUNGGAH DATA KE BIGQUERY
# ==============================================================================
def upload_to_bigquery(df, key_path, project_id, table_id):
    if df is None or df.empty:
        print("Tidak ada data untuk diunggah.")
        return
    print(f"\n📦 Memulai proses unggah ke BigQuery di tabel {table_id}...")
    try:
        credentials = service_account.Credentials.from_service_account_file(key_path)
        client = bigquery.Client(credentials=credentials, project=project_id)
        # WRITE_TRUNCATE: Hapus data lama, lalu masukkan data baru. Ukuran tabel akan stabil.
        job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
        job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
        job.result()
        print(f"✅ Berhasil! {len(df)} baris data telah diunggah ke BigQuery.")
    except Exception as e:
        print(f"❌ Gagal mengunggah data ke BigQuery: {e}")

# ==============================================================================
# 5. BLOK EKSEKUSI UTAMA
# ==============================================================================
if __name__ == "__main__":
    dataframe_wilayah = fetch_wilayah_data(API_KEY, URL_BASE)

    if dataframe_wilayah is not None:
        print(f"\nTotal {len(dataframe_wilayah)} baris data wilayah berhasil dikumpulkan.")
        print("Contoh data:")
        print(dataframe_wilayah.head())
        upload_to_bigquery(dataframe_wilayah, KEY_PATH, PROJECT_ID, TABLE_ID)
    else:
        print("\nProses selesai. Cek pesan error di atas jika ada.")