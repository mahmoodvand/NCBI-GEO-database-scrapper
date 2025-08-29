from scrapers.geo_scraper import scrape_geo
from utils.excel_writer import save_to_excel
import requests
from bs4 import BeautifulSoup
import os
import pandas as pd
import xml.etree.ElementTree as ET


def scrape_and_extract_data(url):
    """
    Scrape data from the HTML page and return it as a dictionary.
    :param url: URL of the HTML page to scrape.
    :return: Dictionary containing the extracted data.
    """
    try:
        # Print the URL being processed
        print(f"Processing URL: {url}")

        # Fetch the HTML content
        response = requests.get(url)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'lxml')

        # Helper function to safely extract text
        def get_text_or_none(label):
            element = soup.find("td", string=label)
            if element:
                sibling = element.find_next_sibling("td")
                return sibling.get_text(strip=True) if sibling else None
            return None

        # Extract data from the HTML page
        data = {
            "Dataset URL": url,
            "Title": get_text_or_none("Title"),
            "Summary": get_text_or_none("Summary"),
            "Experiment type": get_text_or_none("Experiment type"),
            "Overall design": get_text_or_none("Overall design"),
            "Contributor(s)": ", ".join(
                [a.get_text(strip=True) for a in soup.find_all("a", href=True) if "Author" in a.get("href")]
            ),
            "Submission date": get_text_or_none("Submission date"),
            "Last update date": get_text_or_none("Last update date"),
            "Contact name": get_text_or_none("Contact name"),
            "Organization name": get_text_or_none("Organization name"),
            "Street address": get_text_or_none("Street address"),
            "City": get_text_or_none("City"),
            "ZIP/Postal code": get_text_or_none("ZIP/Postal code"),
            "Country": get_text_or_none("Country"),
            "Platforms": ", ".join(
                [a.get_text(strip=True) for a in soup.find_all("a", href=True) if "GPL" in a.get("href")]
            ),
            "Samples": ", ".join(
                [a.get_text(strip=True) for a in soup.find_all("a", href=True) if "GSM" in a.get("href")]
            ),
            "Accession Number": soup.find("strong", {"class": "acc"}).get_text(strip=True) if soup.find("strong", {"class": "acc"}) else None,
        }

        # Display the fetched data in the terminal
        print("[INFO] Fetched Data:")
        for key, value in data.items():
            print(f"{key}: {value}")

        # Save the data to the Excel file
        save_to_excel([data])

    except Exception as e:
        print(f"[ERROR] Error scraping data from {url}: {e}")
        return None


def process_xml_file(xml_file_path):
    """
    Process an XML file to extract sample data and return it as a list of dictionaries.
    :param xml_file_path: Path to the XML file.
    :return: List of dictionaries containing the extracted data for each sample.
    """
    try:
        # Parse the XML file
        tree = ET.parse(xml_file_path)
        root = tree.getroot()

        # Namespace used in the XML file
        namespace = {"ns": "http://www.ncbi.nlm.nih.gov/geo/info/MINiML"}

        # List to store extracted data
        extracted_data = []

        # Iterate through each <Sample> node
        for sample in root.findall(".//ns:Sample", namespace):
            sample_data = {}

            # Extract the sample ID (iid attribute)
            sample_data["Sample Number (ID)"] = sample.get("iid", "N/A")

            # Extract the <Characteristics tag="tissue"> value
            tissue = sample.find(".//ns:Characteristics[@tag='tissue']", namespace)
            sample_data["Tissue"] = tissue.text.strip() if tissue is not None and tissue.text else "N/A"

            # Extract the <Characteristics tag="cell type"> value
            cell_type = sample.find(".//ns:Characteristics[@tag='cell line']", namespace)
            sample_data["Cell Type"] = cell_type.text.strip() if cell_type is not None and cell_type.text else "N/A"

            # Append the sample data to the list
            extracted_data.append(sample_data)

            # Display the extracted data in the terminal
            print("[INFO] Extracted Sample Data:")
            print(f"  Sample Number (ID): {sample_data['Sample Number (ID)']}")
            print(f"  Tissue: {sample_data['Tissue']}")
            print(f"  Cell Type: {sample_data['Cell Type']}")
            print("-" * 50)

        return extracted_data

    except Exception as e:
        print(f"[ERROR] Failed to process XML file {xml_file_path}: {e}")
        return []


def save_to_excel(data, output_file="final_result.xlsx"):
    """
    Save the scraped data to an Excel file.
    :param data: List of dictionaries containing the scraped data.
    :param output_file: Path to the Excel file where data will be saved.
    """
    # Define the column order
    columns = [
        "Dataset URL", "Title", "Summary", "Experiment type", "Overall design",
        "Contributor(s)", "Submission date", "Last update date", "Contact name",
        "Organization name", "Street address", "City", "ZIP/Postal code", "Country",
        "Platforms", "Samples", "Accession Number", "Sample Number (ID)", "Organism", "Tissue", "Cell Type"
    ]

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


def main():
    """
    Main function to scrape GEO data and save it to an Excel file.
    """
    # Scrape data
    results = scrape_geo()

    # Save results to Excel
    save_to_excel(results, base_output_path="data/final_result")


if __name__ == "__main__":
    main()
