import pandas as pd

def count_rows_in_csv(file_path):
    df = pd.read_csv(file_path)  # Read the CSV file
    return len(df)  # Return the number of rows

# Example usage
csv_file_path = 'transcripts.csv'  # Replace with your CSV file path
row_count = count_rows_in_csv(csv_file_path)
print(f'The CSV file has {row_count} rows.')
