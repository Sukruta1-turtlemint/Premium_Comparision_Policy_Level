import os
import pandas as pd

class FileHandler:
    """Handles file reading for CSV and Excel files."""
    def read_csv(self, file_path):
        return pd.read_csv(file_path)
    
    def read_excel(self, file_path, sheet_name=None, header=0, nrows=None):
        ext = os.path.splitext(file_path)[1].lower()
        if ext == '.xlsb':
            return pd.read_excel(file_path, sheet_name=sheet_name, header=header, nrows=nrows, engine='pyxlsb')
        else:
            return pd.read_excel(file_path, sheet_name=sheet_name, header=header, nrows=nrows)
    
    def get_excel_sheet_names(self, file_path):
        ext = os.path.splitext(file_path)[1].lower()
        if ext == '.xlsb':
            return pd.ExcelFile(file_path, engine='pyxlsb').sheet_names
        else:
            return pd.ExcelFile(file_path).sheet_names

class DataExtractor:
    """Extracts data from insurer spreadsheets."""
    def extract_sheet_data(self, file_handler, file_path, sheet_name, year, insurer):
        # Read the first 5 rows without header to find the header row
        sample_df = file_handler.read_excel(file_path, sheet_name=sheet_name, header=None, nrows=5)
        header_row_index = None
        # Look for a row containing both 'Total premium' and 'Policy number' (case-insensitive)
        for i in range(len(sample_df)):
            row_str = ' '.join(sample_df.iloc[i].astype(str).str.lower())
            if 'total premium' in row_str and 'policy number' in row_str:
                header_row_index = i
                break
        # If not found, default to row 0.
        if header_row_index is None:
            header_row_index = 0

        # Load the entire sheet using the discovered header row.
        df = file_handler.read_excel(file_path, sheet_name=sheet_name, header=header_row_index)
        df.columns = df.columns.str.strip().str.lower()
        
        # Verify required columns.
        if 'policy number' not in df.columns or 'total premium' not in df.columns:
            return None
        
        # Build the extracted dataframe.
        extracted = pd.DataFrame({
            'Year': year,
            'Insurer': insurer,
            'Type': sheet_name.lower(),  # Use sheet name as type (in lowercase)
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
            return pd.DataFrame()  # Return empty DataFrame if no valid sheet data is found

class DataComparer:
    """Compares the S3 premium data with the extracted Given premium data."""
    def aggregate_given_data(self, given_df):
        """
        Aggregates premium data from sub-type sheets.
        If the 'Type' column starts with 'base' (or 'reward') then those rows are aggregated
        into a single 'base' (or 'reward') row per Year, Insurer, and Policy number.
        """
        # Standardize column names to lowercase.
        given_df.columns = given_df.columns.str.strip().str.lower()
        # Create an aggregation key: if type starts with 'base' then set as 'base'; if 'reward', then 'reward'
        given_df['agg_type'] = given_df['type'].apply(lambda x: 'base' if x.startswith('base') else ('reward' if x.startswith('reward') else x))
        # Group by year, insurer, aggregated type, and policy number, summing the Premium.
        agg_df = given_df.groupby(['year', 'insurer', 'agg_type', 'policy number'], as_index=False).agg({'premium': 'sum'})
        # Record the datatype of the aggregated premium.
        agg_df['datatype'] = agg_df['premium'].apply(lambda x: type(x).__name__)
        # Rename the aggregated type column back to 'type'.
        agg_df.rename(columns={'agg_type': 'type'}, inplace=True)
        return agg_df
    
    def compare_data(self, s3_df, given_df):
        # Standardize S3 dataframe column names.
        s3_df.columns = s3_df.columns.str.strip().str.lower()

        # Rename the premium column in s3_df from 'total premium' to 'premium'
        s3_df.rename(columns={'total premium': 'premium'}, inplace=True)
        
        # Aggregate Given premium data to consolidate sub-type sheets.
        aggregated_given_df = self.aggregate_given_data(given_df)

        print(s3_df.columns)
        print(aggregated_given_df.columns)

        # Standardize key columns in both DataFrames:
        for col in ['insurer', 'type', 'policy number']:
            s3_df[col] = s3_df[col].astype(str).str.strip().str.lower()

        s3_df['year'] = s3_df['year'].astype(int)  # Convert to int
        aggregated_given_df['year'] = aggregated_given_df['year'].astype(int)  # Convert to int

        # Merge S3 and aggregated Given data on year, insurer, type, and policy number.
        merged = pd.merge(s3_df, aggregated_given_df, on=['year', 'insurer', 'type', 'policy number'],
                          how='outer', suffixes=('_s3', '_given'))
        
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

        print("merged df:", merged)
        
        def record_multivalues(row):
            if row['Status'] == "Not matches":
                return f"[{row.get('total premium')}, {row.get('premium')}]"
            else:
                return ""
        
        merged['Multivalues'] = merged.apply(record_multivalues, axis=1)

        print("merged df:", merged)
        # Rename columns for clarity.
        merged.rename(columns={
            'premium_s3': 'S3_premium',
            'premium_given': 'Given_premium'
        }, inplace=True)

        print("merged df:", merged)
        # Rearranging the columns as specified.
        cols = ['year', 'insurer', 'type', 'policy number', 'S3_premium', 'Given_premium', 'datatype', 'Status', 'Multivalues']
        comparison_df = merged[cols]
        # Capitalize column names for the final output.
        comparison_df.rename(columns={
            'year': 'Year',
            'insurer': 'Insurer',
            'type': 'Type',
            'policy number': 'Policy number',
            'datatype': 'Datatype'
        }, inplace=True)
        return comparison_df

class ReportGenerator:
    """Generates Excel reports for Given premium data and comparison report."""
    def save_given_premium(self, given_df, output_path):
        given_df.to_excel(output_path, index=False)
    
    def save_comparison_report(self, comparison_df, output_path):
        comparison_df.to_excel(output_path, index=False)
        print(f"Comparison report saved to {output_path}")

def main(root_folder, s3_excel_path, given_output_path, comparison_output_path):
    file_handler = FileHandler()
    data_extractor = DataExtractor()
    data_comparer = DataComparer()
    report_generator = ReportGenerator()
    
    # Read S3 premium data from CSV.
    s3_df = file_handler.read_excel(s3_excel_path, sheet_name='Sheet1')
    s3_df.columns = s3_df.columns.str.strip().str.lower()
    
    all_given_data = []   # List because we may have multiple Given premium dataframes to concatenate.
    # Iterate through each year folder.
    for year_folder in os.listdir(root_folder):
        year_folder_path = os.path.join(root_folder, year_folder)
        if os.path.isdir(year_folder_path):
            # Process each Excel file in the year folder.
            for file in os.listdir(year_folder_path):
                if file.lower().endswith(('.xlsx', '.xlsb', '.xls')):
                    file_path = os.path.join(year_folder_path, file)
                    extracted_data = data_extractor.extract_data_from_file(file_handler, file_path, year_folder)
                    if not extracted_data.empty:
                        all_given_data.append(extracted_data)
    
    if all_given_data:
        given_df = pd.concat(all_given_data, ignore_index=True)
    else:
        given_df = pd.DataFrame()
    
    # Save the raw Given premium data report (if needed).
    report_generator.save_given_premium(given_df, given_output_path)
    
    # Compare the S3 and Given premium data (after aggregation) and generate the comparison report.
    comparison_df = data_comparer.compare_data(s3_df, given_df)
    report_generator.save_comparison_report(comparison_df, comparison_output_path)
    print("Reports generated successfully.")

if __name__ == "__main__":
    # Update these paths as needed for your environment.
    root_folder = "/Users/sukrutasakoji/Desktop/Trial"          # Root directory containing year-wise folders
    s3_excel_path = "/Users/sukrutasakoji/Desktop/S3_premium_Testing_Aegon_2024.xlsx"         # S3 premium CSV file path
    given_output_path = "/Users/sukrutasakoji/Desktop/Premium_Comparision_Policy_Level/Given_Report.xlsx"
    comparison_output_path = "/Users/sukrutasakoji/Desktop/Premium_Comparision_Policy_Level/Comparison_Report.xlsx"
    
    main(root_folder, s3_excel_path, given_output_path, comparison_output_path)