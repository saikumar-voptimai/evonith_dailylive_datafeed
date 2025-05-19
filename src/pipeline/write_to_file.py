"""
CSV/TXT file writer for cleaned data.
"""
import csv
import os
import logging

logger = logging.getLogger('pipeline.write_to_file')

def write_to_csv(cleaned_data, table_config, date_str, out_dir='output'):
    """
    Writes cleaned data to CSV files, one per table type.
    Args:
        cleaned_data (list[dict]): Cleaned data records
        table_config (dict): Table/variable config
        date_str (str): Date string for filename
        out_dir (str): Output directory
    """
    os.makedirs(out_dir, exist_ok=True)
    for table, variables in table_config.items():
        filename = os.path.join(out_dir, f"{table}_{date_str}.csv")
        with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=['Timelogged'] + variables)
            writer.writeheader()
            for row in cleaned_data:
                filtered = {'Timelogged': row.get('Timelogged')}
                for var in variables:
                    filtered[var] = row.get(var)
                writer.writerow(filtered)
        logger.info(f"Wrote {filename}")


def write_points_to_txt(points, date_str, mode='daily', out_dir='output'):
    """
    Writes InfluxDB points (in line protocol) to a .txt file for testing.
    Args:
        points (list): List of influxdb_client_3.Point objects
        date_str (str): Date string for filename
        mode (str): 'daily' or 'live'
        out_dir (str): Output directory
    """
    import datetime
    os.makedirs(out_dir, exist_ok=True)
    if mode == 'live':
        now = datetime.datetime.now()
        filename = os.path.join(out_dir, now.strftime(f"%d_%m_%Y_%H_%M_%S.txt"))
    else:
        filename = os.path.join(out_dir, f"{date_str}.txt")
    with open(filename, 'w', encoding='utf-8') as f:
        for point in points:
            f.write(point.to_line_protocol() + '\n')
    logger.info(f"Wrote line protocol points to {filename}")