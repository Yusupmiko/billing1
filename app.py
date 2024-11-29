from flask import Flask, render_template, request, send_file, request, jsonify
import io  # Tambahkan ini untuk mengimpor modul io
from io import BytesIO  # Add this import statement
import pandas as pd
import numpy as np
from bs4 import BeautifulSoup
from sqlalchemy import create_engine, text
import mysql.connector
from flask import request, redirect, url_for, flash  # Make sure to import flash
from flask import Flask, render_template, request, redirect, url_for, flash, session
from flask_mysqldb import MySQL
from werkzeug.security import check_password_hash, generate_password_hash


app = Flask(__name__)



#koneksi
app.secret_key = '040104'
app.config['MYSQL_HOST'] ='telup3singkawang.com'
app.config['MYSQL_USER'] ='telupsin_telupsin'
app.config['MYSQL_PASSWORD'] ='Yusup890.'
app.config['MYSQL_DB'] ='telup_billing'
mysql = MySQL(app)

# Database connection details
db_config = {
    'host': 'telup3singkawang.com',  # Change if necessary
    'user': 'telupsin_telupsin',  # Your MySQL username
    'password': 'Yusup890.',  # Your MySQL password
    'database': 'telup_billing'  # Your MySQL database name
}


#index
@app.route('/')
def index1():
    if 'loggedin' in session:
        return render_template('index.html')
    # flash('','danger')
    return redirect(url_for('login'))


#registrasi
@app.route('/registrasi', methods=('GET','POST'))
def registrasi():
    if request.method == 'POST':
        username = request.form['username']
        email = request.form['email']
        password = request.form['password']
        level = request.form['level']

        #cek username atau email
        cursor = mysql.connection.cursor()
        cursor.execute('SELECT * FROM tb_users WHERE username=%s OR email=%s',(username, email, ))
        akun = cursor.fetchone()
        if akun is None:
            cursor.execute('INSERT INTO tb_users VALUES (NULL, %s, %s, %s, %s)', (username, email, generate_password_hash(password), level))
            mysql.connection.commit()
            flash('Registrasi Berhasil','success')
        else :
            flash('Username atau email sudah ada','danger')
    return render_template('registrasi.html')



# Login
@app.route('/login', methods=('GET', 'POST'))
def login():
    if request.method == 'POST':
        email = request.form['email']
        password = request.form['password']
        
        # cek data username
        cursor = mysql.connection.cursor()
        cursor.execute('SELECT * FROM tb_users WHERE email=%s', (email,))
        akun = cursor.fetchone()
        if akun is None:
            flash('Login Gagal, Cek Username Anda', 'danger')
        elif not check_password_hash(akun[3], password):
            flash('Login Gagal, Cek Password Anda', 'danger')
        else:
            session['loggedin'] = True
            session['username'] = akun[1]
            session['level'] = akun[4]
            return redirect(url_for('index'))
    return render_template('login.html')
# #login
# @app.route('/login', methods=('GET', 'POST'))
# def login():
#     if request.method == 'POST':
#         email = request.form['email']
#         password = request.form['password']
        
#         #cek data username
#         cursor = mysql.connection.cursor()
#         cursor.execute('SELECT * FROM tb_users WHERE email=%s',(email, ))
#         akun = cursor.fetchone()
#         if akun is None:
#             flash('Login Gagal, Cek Username Anda','danger')
#         elif not check_password_hash(akun[3], password):
#             flash('Login gagal, Cek Password Anda', 'danger')
#         else:
#             session['loggedin'] = True
#             session['username'] = akun[1]
#             session['level'] = akun[4]
#             return redirect(url_for('index1'))
#     return render_template('login.html')


# Logout
@app.route('/logout')
def logout():
    session.pop('loggedin', None)
    session.pop('username', None)
    session.pop('level', None)
    return redirect(url_for('login'))

# Middleware untuk proteksi login
def login_required(f):
    def wrapper(*args, **kwargs):
        if not session.get('loggedin'):
            flash('Anda harus login terlebih dahulu', 'warning')
            return redirect(url_for('login'))
        return f(*args, **kwargs)
    wrapper.__name__ = f.__name__
    return wrapper


# #logout
# @app.route('/logout')
# def logout():
#     session.pop('loggedin', None)
#     session.pop('username', None)
#     session.pop('level', None)
#     return redirect(url_for('login'))


# Create engine for MySQL connection
engine = create_engine(f"mysql+pymysql://{db_config['user']}:{db_config['password']}@{db_config['host']}/{db_config['database']}")

def copy_dataframe(lalulalu, lalu, akhir, blth_lalulalu, blth_lalu, blth_kini):
    # Create DataFrames and merge them
    juruslalulalu = pd.DataFrame({
        'BLTH': lalulalu.get('BLTH', pd.Series(dtype='object')),
        'IDPEL': lalulalu.get('IDPEL', pd.Series(dtype='int64')),
        'LWBPPAKAI': lalulalu.get('LWBPPAKAI', pd.Series(dtype='float64')) 
    })

    juruslalu = pd.DataFrame({
        'BLTH': lalu.get('BLTH', pd.Series(dtype='object')),
        'IDPEL': lalu.get('IDPEL', pd.Series(dtype='int64')),
        'LWBPPAKAI': lalu.get('LWBPPAKAI', pd.Series(dtype='float64'))
    })

    jurusakhir = pd.DataFrame({
        'BLTH': akhir.get('BLTH', pd.Series(dtype='object')),
        'IDPEL': akhir.get('IDPEL', pd.Series(dtype='int64')),
        'NAMA': akhir.get('NAMA', pd.Series(dtype='object')),
        'TARIF': akhir.get('TARIF', pd.Series(dtype='object')),
        'DAYA': akhir.get('DAYA', pd.Series(dtype='float64')),
        'SLALWBP': akhir.get('SLALWBP', pd.Series(dtype='float64')),
        'LWBPCABUT': akhir.get('LWBPCABUT', pd.Series(dtype='float64')),
        'LWBPPASANG': akhir.get('LWBPPASANG', pd.Series(dtype='float64')),
        'SAHLWBP': akhir.get('SAHLWBP', pd.Series(dtype='float64')),
        'LWBPPAKAI': akhir.get('LWBPPAKAI', pd.Series(dtype='float64')),
        'DLPD': akhir.get('DLPD', pd.Series(dtype='float64'))
    })

    # Merging DataFrames
    kroscek_temp_1 = pd.merge(juruslalulalu, juruslalu, on='IDPEL', how='right')
    kroscek_temp = pd.merge(kroscek_temp_1, jurusakhir, on='IDPEL', how='right')
    # Menghitung delta
    delta = kroscek_temp['LWBPPAKAI'] - kroscek_temp['LWBPPAKAI_y']

    # Membuat DataFrame akhir
    kroscek = pd.DataFrame({
        'BLTH': blth_kini,
        'IDPEL': kroscek_temp['IDPEL'],
        'NAMA': kroscek_temp['NAMA'],
        'TARIF': kroscek_temp['TARIF'],
        'DAYA': kroscek_temp['DAYA'].fillna(0).astype(int),
        'SLALWBP': kroscek_temp['SLALWBP'].fillna(0).astype(int),
        'LWBPCABUT': kroscek_temp['LWBPCABUT'].fillna(0).astype(int),
        'SELISIH STAN BONGKAR': (kroscek_temp['SLALWBP'].fillna(0) - kroscek_temp['LWBPCABUT'].fillna(0)).astype(int),
        'LWBP PASANG': kroscek_temp['LWBPPASANG'].fillna(0).astype(int),
        'SAHLWBP': kroscek_temp['SAHLWBP'].fillna(0).astype(int),
        'KWH 10': kroscek_temp['LWBPPAKAI'].fillna(0).astype(int),
        'KWH 09': kroscek_temp['LWBPPAKAI_y'].fillna(0).astype(int),
        'KWH 08': kroscek_temp['LWBPPAKAI_x'].fillna(0).astype(int),
        'DELTA PEMKWH': delta.fillna(0).astype(int)
    })

    # Menghitung persentase
    percentage = (delta / kroscek_temp['LWBPPAKAI_y'].replace(0, np.nan)) * 100  # Menghindari pembagian oleh 0

    # Mengatasi NaN atau inf di persentase dengan mengganti menjadi 0
    percentage = np.nan_to_num(percentage, nan=0, posinf=0, neginf=0)

    # Konversi persentase menjadi kolom di DataFrame, lalu format dengan simbol '%'
    kroscek['%'] = pd.Series(percentage).astype(int).astype(str) + '%'

    # Menyimpan kolom KET dengan kondisi yang sudah ditentukan
    kroscek['KET'] = np.where(
        percentage >= 40, 'NAIK', 
        np.where(percentage <= -40, 'TURUN', 'AMAN')
    )

    # Memastikan nilai KET adalah 'AMAN' jika persentase adalah 0
    kroscek.loc[percentage == 0, 'KET'] = 'AMAN'

    # Menambahkan kolom DLPD
    kroscek['DLPD'] = kroscek_temp['DLPD']


    # URLs for images
    path_foto1 = 'https://portalapp.iconpln.co.id/acmt/DisplayBlobServlet1?idpel='
    path_foto2 = '&blth='
    kroscek['FOTO AKHIR'] = kroscek['IDPEL'].apply(lambda x: f'<a href="{path_foto1}{x}{path_foto2}{blth_kini}" target="_blank">LINK FOTO</a>')
    kroscek['FOTO LALU'] = kroscek['IDPEL'].apply(lambda x: f'<a href="{path_foto1}{x}{path_foto2}{blth_lalu}" target="_blank">LINK FOTO</a>')
    kroscek['FOTO LALU2'] = kroscek['IDPEL'].apply(lambda x: f'<a href="{path_foto1}{x}{path_foto2}{blth_lalulalu}" target="_blank">LINK FOTO</a>')

    kroscek['HASIL PEMERIKSAAN'] = kroscek['KET'].apply(lambda x: f'<select class="hasil-pemeriksaan" onfocus="this.options[0].selected = true;">'
                                                      '<option value="" disabled selected hidden></option>'
                                                      '<option value="SESUAI" {"selected" if x == "SESUAI" else ""}>SESUAI</option>'
                                                      '<option value="SALAH STAN" {"selected" if x == "SALAH STAN" else ""}>SALAH STAN</option>'
                                                      '<option value="TELAT/SALAH PDL" {"selected" if x == "TELAT/SALAH PDL" else ""}>TELAT/SALAH PDL</option>'
                                                      '<option value="SALAH FOTO" {"selected" if x == "SALAH FOTO" else ""}>SALAH FOTO</option>'
                                                      '<option value="FOTO BURAM" {"selected" if x == "FOTO BURAM" else ""}>FOTO BURAM</option>'
                                                      '<option value="LEBIH TAGIH" {"selected" if x == "LEBIH TAGIH" else ""}>LEBIH TAGIH</option>'
                                                      '<option value="BUKAN FOTO KWH" {"selected" if x == "BUKAN FOTO KWH" else ""}>BUKAN FOTO KWH</option>'
                                                      '<option value="BENCANA" {"selected" if x == "BENCANA" else ""}>BENCANA</option>'
                                                      '</select>')

    kroscek['TINDAK LANJUT'] = '<textarea class="tindak-lanjut" rows="4" cols="50"></textarea>'

    kroscek['KETERANGAN'] = kroscek['KET'].apply(lambda x: '<select class="keterangan" onfocus="this.options[0].selected = true;">'
                                                       '<option value="" disabled selected hidden></option>'
                                                       '<option value="3 BULAN TIDAK DAPAT FOTO STAN">3 BULAN TIDAK DAPAT FOTO STAN</option>' 
                                                       '<option value="6 BULAN TIDAK DAPAT FOTO STAN">6 BULAN TIDAK DAPAT FOTO STAN</option>' 
                                                       '<option value="SUDAH BU">SUDAH BU</option>'
                                                       '<option value="SALAH FOTO">SALAH FOTO</option>'
                                                       '<option value="720">720</option>'
                                                       '</select>')

    
    return kroscek

def naikFilter(lalulalu, lalu, akhir, blth_lalulalu, blth_lalu, blth_kini):
    kroscek = copy_dataframe(lalulalu, lalu, akhir, blth_lalulalu, blth_lalu, blth_kini)
    naik_df = kroscek[kroscek['%'].str.rstrip('%').astype(int) >= 40]
    return naik_df

def turunFilter(lalulalu, lalu, akhir, blth_lalulalu, blth_lalu, blth_kini):
    kroscek = copy_dataframe(lalulalu, lalu, akhir, blth_lalulalu, blth_lalu, blth_kini)
    turun_df = kroscek[kroscek['%'].str.rstrip('%').astype(int) <= -40]
    return turun_df

def amanFilter(lalulalu, lalu, akhir, blth_lalulalu, blth_lalu, blth_kini):
    kroscek = copy_dataframe(lalulalu, lalu, akhir, blth_lalulalu, blth_lalu, blth_kini)
    aman_df = kroscek[(kroscek['%'].str.rstrip('%').astype(int) < 40) & (kroscek['%'].str.rstrip('%').astype(int) > -40)]
    return aman_df

# Fungsi untuk menyimpan DataFrame ke MySQL
def save_to_database(df, table_name):
    try:
        # Simpan DataFrame ke tabel MySQL
        df.to_sql(table_name, engine, if_exists='replace', index=False)
        print(f"Data berhasil disimpan ke tabel '{table_name}'")
    except Exception as e:
        print(f"Terjadi kesalahan saat menyimpan data ke database: {e}")

@app.route("/save_to_db", methods=["POST"])
def save_to_db():
    global result_df, naik_df, turun_df, aman_df  # Pastikan DataFrame global bisa diakses

    try:
        # Simpan DataFrame ke MySQL
        save_to_database(result_df, 'billing_result')
        save_to_database(naik_df, 'billing_naik')
        save_to_database(turun_df, 'billing_turun')
        save_to_database(aman_df, 'billing_aman')
        return {"status": "success", "message": "Data berhasil disimpan ke database"}
    except Exception as e:
        return {"status": "error", "message": f"Terjadi kesalahan: {e}"}

# Global DataFrames to store results
result_df = None
naik_df = None
turun_df = None
aman_df = None





@app.route("/", methods=["GET", "POST"])
def index():
    
    if "username" not in session:
        return redirect(url_for("login"))
    
    global result_df, naik_df, turun_df, aman_df  # Use global to access DataFrames across routes

    if request.method == "POST":
        blth_lalulalu = request.form['blth_lalulalu']
        blth_lalu = request.form['blth_lalu']
        blth_kini = request.form['blth_kini']

        file_lalulalu = request.files['file_lalulalu']
        file_lalu = request.files['file_lalu']
        file_akhir = request.files['file_akhir']

        if file_lalulalu and file_lalu and file_akhir:
            lalulalu = pd.read_excel(file_lalulalu)
            lalu = pd.read_excel(file_lalu)
            akhir = pd.read_excel(file_akhir)

            # Process the data
            result_df = copy_dataframe(lalulalu, lalu, akhir, blth_lalulalu, blth_lalu, blth_kini)
            result_df.sort_values(by='DELTA PEMKWH', ascending=False, inplace=True)
            result_df.sort_values(by='%', ascending=False, inplace=True)
            
            # Filter data
            naik_df = naikFilter(lalulalu, lalu, akhir, blth_lalulalu, blth_lalu, blth_kini)
            turun_df = turunFilter(lalulalu, lalu, akhir, blth_lalulalu, blth_lalu, blth_kini)
            aman_df = amanFilter(lalulalu, lalu, akhir, blth_lalulalu, blth_lalu, blth_kini)

            # # Simpan DataFrame ke MySQL
            save_to_database(result_df, 'billing_result')
            save_to_database(naik_df, 'billing_naik')
            save_to_database(turun_df, 'billing_turun')
            save_to_database(aman_df, 'billing_aman')

            # Convert DataFrames to HTML
            result_html = result_df.to_html(escape=False)
            naik_html = naik_df.to_html(escape=False)
            turun_html = turun_df.to_html(escape=False)
            aman_html = aman_df.to_html(escape=False)

            return render_template('index.html', result=result_html, naik=naik_html, turun=turun_html, aman=aman_html)

    return render_template('index.html', result=None, naik=None, turun=None, aman=None)



# View Data (Proteksi login ditambahkan)
@app.route("/view_data")
@login_required
def view_data():
    # Query data dari tabel billing_result, billing_naik, billing_turun, dan billing_aman
    data_result = pd.read_sql("SELECT * FROM billing_result", engine)
    data_naik = pd.read_sql("SELECT * FROM billing_naik", engine)
    data_turun = pd.read_sql("SELECT * FROM billing_turun", engine)
    data_aman = pd.read_sql("SELECT * FROM billing_aman", engine)

    # Konversi DataFrames ke HTML
    result_html = data_result.to_html(classes="table table-striped", index=False, escape=False)
    naik_html = data_naik.to_html(classes="table table-striped", index=False, escape=False)
    turun_html = data_turun.to_html(classes="table table-striped", index=False, escape=False)
    aman_html = data_aman.to_html(classes="table table-striped", index=False, escape=False)

    # Render template view_data.html dengan data HTML yang telah dikonversi
    return render_template(
        "view_data.html",
        result_html=result_html,
        naik_html=naik_html,
        turun_html=turun_html,
        aman_html=aman_html
    )




# @app.route("/view_data")
# def view_data():
#     # Query data dari tabel billing_result, billing_naik, billing_turun, dan billing_aman
#     data_result = pd.read_sql("SELECT * FROM billing_result", engine)
#     data_naik = pd.read_sql("SELECT * FROM billing_naik", engine)
#     data_turun = pd.read_sql("SELECT * FROM billing_turun", engine)
#     data_aman = pd.read_sql("SELECT * FROM billing_aman", engine)

#     # Konversi DataFrames ke HTML
#     result_html = data_result.to_html(classes="table table-striped", index=False, escape=False)
#     naik_html = data_naik.to_html(classes="table table-striped", index=False, escape=False)
#     turun_html = data_turun.to_html(classes="table table-striped", index=False, escape=False)
#     aman_html = data_aman.to_html(classes="table table-striped", index=False, escape=False)

#     # Render template view_data.html dengan data HTML yang telah dikonversi
#     return render_template(
#         "view_data.html",
#         result_html=result_html,
#         naik_html=naik_html,
#         turun_html=turun_html,
#         aman_html=aman_html
#     )

@app.route('/')
def main_dashboard():
    # Your main code to render data tables
    return render_template('view_data.html')

def get_db_connection():
    return mysql.connector.connect(**db_config)

@app.route('/download_data/<table>')
def download_data(table):
    # Query data from the specified table
    query = f"SELECT * FROM {table}"

    # Get data from the database
    data = pd.read_sql(query, engine)

    # Create Excel file in memory
    output = BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        data.to_excel(writer, index=False, sheet_name=table)
        # No need to call save() or close() explicitly; using 'with' handles it
    output.seek(0)

    # Send file
    return send_file(
        output,
        as_attachment=True,
        download_name=f"{table}_data.xlsx",
        mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
    )
    
# Set a secret key for session management (required for flash messages)
app.secret_key = '040104'  # Replace with a strong, unique key

@app.route("/delete_data", methods=["POST"])
def delete_data():
    try:
        table = request.json.get('table')  # Mengambil nama tabel dari request
        if not table:
            return jsonify({"error": "Nama tabel tidak ditemukan"}), 400
        
        # Membuat query DELETE menggunakan text
        delete_query = text(f"DELETE FROM {table}")  # Gunakan text() untuk query SQL

        with engine.connect() as connection:
            # Eksekusi query DELETE terlebih dahulu
            result = connection.execute(delete_query)

            # Periksa jika tidak ada data yang dihapus
            if result.rowcount == 0:
                return jsonify({"error": "Tidak ada data yang dihapus"}), 400

            # Query ALTER TABLE untuk reset auto increment (Hanya untuk MySQL atau MariaDB)
            try:
                alter_query = text(f"ALTER TABLE {table} AUTO_INCREMENT = 1")
                connection.execute(alter_query)  # Eksekusi ALTER TABLE
            except Exception as alter_error:
                # Jika ALTER gagal, tampilkan pesan bahwa reset auto increment gagal
                print(f"Gagal mereset auto increment: {alter_error}")
                return jsonify({"message": "Data berhasil dihapus, namun gagal mereset auto increment."}), 200

        # Jika berhasil menghapus data dan mereset auto increment, beri pesan sukses
        return jsonify({"message": "Data berhasil dihapus"}), 200

    except Exception as e:
        # Jika ada kesalahan yang terjadi, cetak kesalahan namun kembalikan error spesifik
        print(f"Terjadi kesalahan: {str(e)}")  # Log kesalahan
        return jsonify({"error": "Terjadi kesalahan saat menghapus data."}), 500

    
@app.route('/update_data/<table>', methods=['POST'])
def update_data(table):
    try:
        updated_data = request.form.to_dict()  # Retrieve and prepare updated data from the form

        # Logic to update data based on the table parameter
        if table == 'billing_result':
            pass  # Update logic for billing_result
        elif table == 'billing_naik':
            pass  # Update logic for billing_naik
        elif table == 'billing_turun':
            pass  # Update logic for billing_turun
        elif table == 'billing_aman':
            pass  # Update logic for billing_aman
        else:
            flash("Invalid table name.")
            return redirect(url_for('view_data'))

        flash("Data updated successfully!")
    except Exception as e:
        flash(f"Error updating data: {str(e)}")
    
    return redirect(url_for('view_data'))

if __name__ == '__main__':
    #app.run(debug=True)
    
    #ip untuk menjalakan di perangkat lain
    app.run(host='0.0.0.0', port=5001)  
