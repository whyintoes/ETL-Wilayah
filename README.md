Mengolah Data Wilayah Indonesia dengan Python: Membangun ETL Pipeline ke BigQuery
1. Pendahuluan
Dalam proyek ini saya membangun pipeline ETL (Extract, Transform, Load) menggunakan Python. Data diambil dari API Wilayah Indonesia, ditransformasikan dengan pandas, lalu dimuat ke Google BigQuery. File utama bernama PortoETL.py.
 
2. Extract – Mengambil Data dari API
Pipeline melakukan request ke API GoAPI (regional endpoint) dengan API Key.
import requests
import pandas as pd

API_KEY = "34edec7d-e4ba-596f-966b-6bb02f2d"
URL_BASE = "https://api.goapi.io/regional"

def extract_data(endpoint):
    url = f"{URL_BASE}/{endpoint}?api_key={API_KEY}"
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()
        return pd.DataFrame(data["data"])
    else:
        print(f"Gagal mengambil data: {response.status_code}")
        return pd.DataFrame()
 
3. Transform – Membersihkan dan Menyiapkan Data
Transformasi sederhana: menghapus duplikasi, normalisasi kolom, dan menambahkan metadata.
def transform_data(df, level):
    df = df.drop_duplicates()
    df["level"] = level
    df["created_at"] = pd.Timestamp.now()
    return df
 
4. Load – Menyimpan ke Google BigQuery
Pipeline menggunakan service account JSON key untuk autentikasi.
from google.cloud import bigquery
from google.oauth2 import service_account

KEY_PATH = "/Users/wahyues/DS PWDK/Porto ETL/project-api-wilayah-1414-c5882d43ac99.json"
PROJECT_ID = "project-api-wilayah-1414"
TABLE_ID = "dataset_wilayah"

credentials = service_account.Credentials.from_service_account_file(KEY_PATH)
client = bigquery.Client(credentials=credentials, project=PROJECT_ID)

def load_to_bigquery(df, table_name):
    table_id = f"{PROJECT_ID}.{TABLE_ID}.{table_name}"
    job = client.load_table_from_dataframe(df, table_id)
    job.result()
    print(f"Data berhasil dimuat ke {table_id}")
 
5. Menjalankan Pipeline
Pipeline dijalankan untuk setiap level wilayah: provinsi, kota, kecamatan, kelurahan.
import time

def run_pipeline():
    for endpoint in ["provinsi", "kota", "kecamatan", "kelurahan"]:
        df = extract_data(endpoint)
        if not df.empty:
            df_transformed = transform_data(df, endpoint)
            load_to_bigquery(df_transformed, endpoint)
            time.sleep(2)  # jeda agar tidak overload API

if __name__ == "__main__":
    run_pipeline()
 
6. Kelebihan Pendekatan Ini
•	Terintegrasi penuh: data real-time dari API langsung masuk ke BigQuery.
•	Modular: fungsi ETL dipisahkan per tahap, mudah diperluas.
•	Reproducible: hasil konsisten setiap kali pipeline dijalankan.
•	Efisien: memanfaatkan jeda otomatis untuk menghindari rate limit API.
 
7. Rencana Pengembangan
1.	Logging & Error Handling: menambahkan sistem logging untuk memantau kegagalan request atau load.
2.	Otomatisasi Jadwal: menggunakan Airflow atau Cloud Composer agar pipeline berjalan terjadwal.
3.	Notifikasi: integrasi Slack/Telegram untuk memberi tahu status pipeline.
4.	Visualisasi: dashboard interaktif di Looker Studio langsung dari tabel BigQuery.
 
8. Penutup
Dengan Python, kita bisa membangun pipeline ETL yang sederhana namun kuat: dari API publik → transformasi → BigQuery. Pendekatan ini menjadikan data wilayah Indonesia lebih mudah dikelola dan siap dipakai untuk analisis maupun visualisasi.
 
Recommendation: gunakan judul yang lebih menarik, misalnya “Mengolah Data Wilayah Indonesia dengan Python: ETL Pipeline ke BigQuery” atau “Membangun Data Pipeline Wilayah Indonesia dengan Python dan BigQuery”.
Next step: tambahkan diagram alur (API → Pandas → BigQuery) serta unggah kode ke GitHub agar pembaca bisa mencoba sendiri.

