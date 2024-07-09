import os
import zipfile
import pandas as pd
from multiprocessing import Pool

zip_directory = 'D:\\test\\Que1' # put your file location here 
output_csv = 'combined_output1.csv'
keyword = '60d_DAM_PTPObligationBidAwards'

def extract_and_read_csv(zip_file):
    with zipfile.ZipFile(zip_file, 'r') as z:
        csv_filename = next((f for f in z.namelist() if keyword in f), None)
        if csv_filename:
            with z.open(csv_filename) as f:
                return pd.read_csv(f)
    return None

def process_zip_files(zip_files):
    with Pool() as pool:
        dfs = pool.map(extract_and_read_csv, zip_files)
    dfs = [df for df in dfs if df is not None]
    combined_df = pd.concat(dfs, ignore_index=True)
    return combined_df

def main():
    zip_files = [os.path.join(zip_directory, f) for f in os.listdir(zip_directory) if f.endswith('.zip')]
    combined_df = process_zip_files(zip_files)
    combined_df.to_csv(output_csv, index=False)
    print(f"Combined data written to {output_csv}")

if __name__ == "__main__":
    main()
