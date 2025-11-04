import pandas as pd
import os
import zipfile
from datetime import datetime
import re


def extract_files(parquet_main_category, parquet_sub_category, zip_date='tennis_data.zip', start_date='20240201', end_date='20240331'):
    raw_datas_path = '../data/raw'
    main_file_path = os.path.join(raw_datas_path, zip_date)
    zips_saving_path = os.path.join(raw_datas_path, 'zips')
    parquet_saving_path = os.path.join(raw_datas_path, 'extracted')

    start_date = datetime.strptime(start_date, '%Y%m%d')
    end_date = datetime.strptime(end_date, '%Y%m%d')

    with zipfile.ZipFile(main_file_path, 'r') as zf:
        all_files = sorted(zf.namelist())
        print(f"Found {len(all_files)} files in main zip")
        zf.extractall(zips_saving_path)

    print(f'Extracting parquets in date range from {start_date} to {end_date} '
          f'with category' f' --> {parquet_main_category} and subcategory --> {parquet_sub_category}')
    for zip_date in os.listdir(zips_saving_path):
        file_date = datetime.strptime(zip_date.split('.')[0], '%Y%m%d')

        if start_date <= file_date <= end_date:
            zip_full_path = os.path.join(zips_saving_path, zip_date)
            with zipfile.ZipFile(zip_full_path, 'r') as zf:
                files_in_zip = zf.namelist()

                for parquet_name in files_in_zip:
                    pattern = rf'.*/raw_{parquet_main_category}_parquet/{parquet_sub_category}_\d+.parquet'
                    if re.fullmatch(pattern, parquet_name):
                        clean_rel_path = parquet_name.replace(f'../../data/raw/raw_{parquet_main_category}_parquet/',
                                                              '')
                        dest_path = os.path.join(parquet_saving_path, parquet_main_category, parquet_sub_category,
                                                 clean_rel_path)

                        os.makedirs(os.path.dirname(dest_path), exist_ok=True)

                        print(f'Extracting {clean_rel_path}')
                        with zf.open(parquet_name) as src, open(dest_path, 'wb') as dst:
                            dst.write(src.read())


def create_parquet_files(parquet_main_category, parquet_sub_category):
    raw_datas_path = '../data/raw'
    datas_path = os.path.join(raw_datas_path, 'extracted', parquet_main_category, parquet_sub_category)
    save_dir = os.path.join(raw_datas_path, 'parquets')
    save_path = os.path.join(save_dir, f'{parquet_main_category}_{parquet_sub_category}.parquet')

    os.makedirs(save_dir, exist_ok=True)

    all_files = os.listdir(datas_path)

    dfs = []
    print(f'Start creating parquet file...')
    for parquet_name in all_files:
        parquet_path = os.path.join(datas_path, parquet_name)
        df = pd.read_parquet(parquet_path, engine='pyarrow')
        dfs.append(df)

    result = pd.concat(dfs, ignore_index=True)
    print("Saving to:", save_path)
    result.to_parquet(path=save_path, index=False)

    return result


if __name__ == '__main__':
    main_category = 'odds'
    sub_category = 'odds'

    extract_files(
        parquet_main_category=main_category,
        parquet_sub_category=sub_category,
        end_date='20240201'
    )
    create_parquet_files(
        parquet_main_category=main_category,
        parquet_sub_category=sub_category
    )