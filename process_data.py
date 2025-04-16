import pandas as pd
import sqlite3


def process_data():
    xls_url = 'https://ohiodnr.gov/static/documents/oil-gas/production/20210309_2020_1%20-%204.xls'
    file_name = '20210309_2020.xls'

    try:
        print("Trying to download file from URL...")
        xls = pd.ExcelFile(xls_url)
        df = pd.read_excel(xls, sheet_name=0)
        print(f"Downloaded and loaded data from URL successfully. Available sheets: {xls.sheet_names}")
    except Exception as e:
        print(f"Failed to load file from URL. Error: {e}")
        print(f"Trying to load file from local file: {file_name}")
        xls = pd.ExcelFile(file_name)
        df = pd.read_excel(xls, sheet_name=0)
        print(f"Loaded data from local file. Available sheets: {xls.sheet_names}")

    # Clean and print column names
    df.columns = df.columns.str.strip().str.upper()
    print("Cleaned Columns:", df.columns.tolist())

    # Continue with processing
    df['API WELL NUMBER'] = df['API WELL  NUMBER'].astype(str)
    annual_data = df.groupby('API WELL NUMBER')[['OIL', 'GAS', 'BRINE']].sum().reset_index()

    conn = sqlite3.connect('production.db')
    cursor = conn.cursor()

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS annual_production (
            api_well_number TEXT PRIMARY KEY,
            oil INTEGER,
            gas INTEGER,
            brine INTEGER
        )
    """)

    for _, row in annual_data.iterrows():
        cursor.execute("""
            INSERT OR REPLACE INTO annual_production (api_well_number, oil, gas, brine)
            VALUES (?, ?, ?, ?)
        """, (row['API WELL NUMBER'], row['OIL'], row['GAS'], row['BRINE']))

    conn.commit()
    conn.close()

    print("Data processed and stored in SQLite successfully!")


if __name__ == '__main__':
    process_data()
