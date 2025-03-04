import os
import pandas as pd

class FileHandler:
    """Handles file reading for CSV and Excel files."""
    def read_csv(self, file_path):
        """Reads a CSV file."""
        return pd.read_csv(file_path)
    
    def read_excel(self, file_path, sheet_name=None, header='infer', nrows=None):
        """Reads an Excel file. Uses pyxlsb engine for .xlsb files."""
        ext = os.path.splitext(file_path)[1].lower()
        if ext == '.xlsb':
            return pd.read_excel(file_path, sheet_name=sheet_name, header=header, nrows=nrows, engine='pyxlsb')
        else:
            return pd.read_excel(file_path, sheet_name=sheet_name, header=header, nrows=nrows)
    
    def get_excel_sheet_names(self, file_path):
        """Lists sheet names in an Excel file."""
        ext = os.path.splitext(file_path)[1].lower()
        if ext == '.xlsb':
            return pd.ExcelFile(file_path, engine='pyxlsb').sheet_names
        else:
            return pd.ExcelFile(file_path).sheet_names

class DataExtractor:
    """Extracts data from insurer spreadsheets."""
    def extract_sheet_data(self, file_handler, file_path, sheet_name, year, insurer):
        # Read first 5 rows without header to find header row
        sample_df = file_handler.read_excel(file_path, sheet_name=sheet_name, header=None, nrows=5)
        header_row_index = None
        # Look for a row containing both 'Total premium' and 'Policy number' (case-insensitive)
        for i in range(len(sample_df)):
            row_str = ' '.join(sample_df.iloc[i].astype(str).str.lower())
            if 'total premium' in row_str and 'policy number' in row_str:
                header_row_index = i
                break
        # If not found, you may decide to skip the sheet or default to row 0.
        if header_row_index is None:
            header_row_index = 0

        # Now load the entire sheet using the discovered header row.
        df = file_handler.read_excel(file_path, sheet_name=sheet_name, header=header_row_index)
        # Standardize column names
        df.columns = df.columns.str.strip().str.lower()
        
        # Verify that required columns are present.
        if 'policy number' not in df.columns or 'total premium' not in df.columns:
            return None
        
        # Extract and create a dataframe with required columns and metadata.
        extracted = pd.DataFrame({
            'Year': year,
            'Insurer': insurer,
            'Type': sheet_name.lower(),  # Using sheet name as type (converted to lowercase)
            'Policy number': df['policy number'],
            'Premium': df['total premium']
        })
        # Record the datatype of the Total premium column.
        extracted['Datatype'] = df['total premium'].apply(lambda x: type(x).__name__)
        
        return extracted

    def extract_data_from_file(self, file_handler, file_path, year):
        """Processes an Excel file, extracting data from sheets starting with 'base' or 'reward'."""
        insurer = os.path.splitext(os.path.basename(file_path))[0]
        sheet_names = file_handler.get_excel_sheet_names(file_path)
        extracted_frames = []
        for sheet in sheet_names:
            if sheet.lower().startswith('base') or sheet.lower().startswith('reward'):
                df_sheet = self.extract_sheet_data(file_handler, file_path, sheet, year, insurer)
                if df_sheet is not None:
                    extracted_frames.append(df_sheet)
        if extracted_frames:
            return pd.concat(extracted_frames, ignore_index=True)
        else:
            return pd.DataFrame()  # Return empty DataFrame if no sheet data is found

class DataComparer:
    """Compares the S3 premium data with the extracted Given premium data."""
    def compare_data(self, s3_df, given_df):
        # Standardize column names for merging (ensure keys match)
        s3_df.columns = s3_df.columns.str.strip().str.lower()
        # For this example, assume s3_df has 'total premium' as premium column.
        # Merge on Year, Insurer, Type, and Policy number using an outer join.
        merged = pd.merge(s3_df, given_df, on=['year', 'insurer', 'type', 'policy number'], how='outer', suffixes=('_s3', '_given'))
        
        def determine_status(row):
            s3_premium = row.get('total premium')
            given_premium = row.get('premium')
            if pd.isnull(s3_premium) and not pd.isnull(given_premium):
                return "Missing in S3"
            elif pd.isnull(given_premium) and not pd.isnull(s3_premium):
                return "Missing in Given"
            elif pd.isnull(s3_premium) and pd.isnull(given_premium):
                return "Both Missing"
            elif s3_premium == given_premium:
                return "Matches"
            else:
                return "Not matches"
        
        merged['Status'] = merged.apply(determine_status, axis=1)
        
        def record_multivalues(row):
            if row['Status'] == "Not matches":
                return f"S3: {row.get('total premium')} | Given: {row.get('premium')}"
            else:
                return ""
        
        merged['Multivalues'] = merged.apply(record_multivalues, axis=1)
        # Rename columns for output clarity.
        merged.rename(columns={
            'total premium': 'S3_premium',
            'premium': 'Given_premium'
        }, inplace=True)
        # Rearranging the columns as specified.
        cols = ['year', 'insurer', 'type', 'policy number', 'S3_premium', 'Given_premium', 'Datatype', 'Status', 'Multivalues']
        comparison_df = merged[cols]
        # Optionally, rename columns to have initial capitals.
        comparison_df.rename(columns={
            'year': 'Year',
            'insurer': 'Insurer',
            'type': 'Type',
            'policy number': 'Policy number'
        }, inplace=True)
        return comparison_df

class ReportGenerator:
    """Generates Excel reports for Given premium data and comparison report."""
    def save_given_premium(self, given_df, output_path):
        given_df.to_excel(output_path, index=False)
    
    def save_comparison_report(self, comparison_df, output_path):
        comparison_df.to_excel(output_path, index=False)

# Main process that orchestrates the file iteration, data extraction, comparison, and report generation.
def main(root_folder, s3_csv_path, given_output_path, comparison_output_path):
    file_handler = FileHandler()
    data_extractor = DataExtractor()
    data_comparer = DataComparer()
    report_generator = ReportGenerator()
    
    # Read S3 premium data from CSV.
    s3_df = file_handler.read_csv(s3_csv_path)
    s3_df.columns = s3_df.columns.str.strip().str.lower()  # Standardize column names

    all_given_data = []
    # Iterate through each year folder.
    for year_folder in os.listdir(root_folder):
        year_folder_path = os.path.join(root_folder, year_folder)
        if os.path.isdir(year_folder_path):
            # For each file in the year folder that is an Excel workbook.
            for file in os.listdir(year_folder_path):
                if file.lower().endswith(('.xlsx', '.xlsb', '.xls')):
                    file_path = os.path.join(year_folder_path, file)
                    extracted_data = data_extractor.extract_data_from_file(file_handler, file_path, year_folder)
                    if not extracted_data.empty:
                        all_given_data.append(extracted_data)
    
    # Combine all extracted data.
    if all_given_data:
        given_df = pd.concat(all_given_data, ignore_index=True)
    else:
        given_df = pd.DataFrame()
    
    # Save the Given premium data report.
    report_generator.save_given_premium(given_df, given_output_path)
    
    # Compare the S3 and Given premium data.
    comparison_df = data_comparer.compare_data(s3_df, given_df)
    report_generator.save_comparison_report(comparison_df, comparison_output_path)
    print("Reports generated successfully.")

if __name__ == "__main__":
    # Define paths (update these paths as needed)
    root_folder = "path_to_year_folders"            # Root directory containing year-wise folders
    s3_csv_path = "S3_premium.csv"                    # CSV file path
    given_output_path = "Given_policynumber_premium.xlsx"
    comparison_output_path = "Comparison_Report.xlsx"
    
    main(root_folder, s3_csv_path, given_output_path, comparison_output_path)
