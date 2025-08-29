import pandas as pd
import os

def save_to_excel(data, base_output_path="data/final_result"):
    """
    Save the scraped data to an Excel file. If the file size exceeds 50 MB, create a new file.
    :param data: List of dictionaries containing the scraped data.
    :param base_output_path: Base path for the Excel file (without extension).
    """
    # Define the column order
    columns = [
        "Dataset URL", "Title", "Summary", "Experiment type", "Overall design",
        "Contributor(s)", "Submission date", "Last update date", "Contact name",
        "Organization name", "Street address", "City", "ZIP/Postal code", "Country",
        "Platforms", "Samples", "Accession Number", "Sample Number (ID)", "Organism", "Tissue", "Cell Type"
    ]

    # Determine the current file name
    file_index = 1
    output_file = f"{base_output_path}_{file_index}.xlsx"

    # Find the next available file if the current one exceeds 50 MB
    while os.path.exists(output_file) and os.path.getsize(output_file) > 50 * 1024 * 1024:  # 50 MB in bytes
        file_index += 1
        output_file = f"{base_output_path}_{file_index}.xlsx"

    # Check if the file already exists
    if not os.path.exists(output_file):
        # Create a new DataFrame with the specified columns
        df = pd.DataFrame(columns=columns)
        df.to_excel(output_file, index=False, engine="openpyxl")

    # Load the existing Excel file
    df = pd.read_excel(output_file, engine="openpyxl")

    # Append the new data as rows
    new_data_df = pd.DataFrame(data)
    df = pd.concat([df, new_data_df], ignore_index=True)

    # Ensure all columns are present in the final DataFrame
    for column in columns:
        if column not in df.columns:
            df[column] = None

    # Save the updated DataFrame back to the Excel file
    df.to_excel(output_file, index=False, engine="openpyxl")
    print(f"[INFO] Data saved to {output_file}")
