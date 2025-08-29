import requests
from bs4 import BeautifulSoup
from urllib.parse import urlparse, parse_qs
from config import GEO_SEARCH_URLS
import time
from utils.excel_writer import save_to_excel
from utils.file_handler import FileHandler
import xml.etree.ElementTree as ET
import os
import tarfile
from tqdm import tqdm
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Define additional database URLs
ADDITIONAL_DATABASES = {
    "10xgenomics": "https://www.10xgenomics.com/resources/datasets",
    "Single Cell Portal": "https://singlecell.broadinstitute.org/single_cell",
    "Single Cell Expression Atlas": "https://www.ebi.ac.uk/gxa/sc/home",
    "snRNAseqDB": "http://bis.zju.edu.cn/MCA/",
    "CancerSCEM": "https://ngdc.cncb.ac.cn/cancerscem/",
    "Array Express": "https://www.ebi.ac.uk/arrayexpress/"
}

def extract_detailed_features(dataset_soup):
    # Helper function to find the next sibling text of a given label
    def get_next_sibling_text(soup, label):
        label_td = soup.find("td", text=label)
        if label_td and label_td.find_next_sibling("td"):
            return label_td.find_next_sibling("td").text.strip()
        return ""

    return {
        "Dataset URL": get_next_sibling_text(dataset_soup, "Dataset URL"),
        "Keyword": get_next_sibling_text(dataset_soup, "Keyword"),
        "Title": get_next_sibling_text(dataset_soup, "Title"),
        "Summary": get_next_sibling_text(dataset_soup, "Summary"),
        "Experiment type": get_next_sibling_text(dataset_soup, "Experiment type"),
        "Overall design": get_next_sibling_text(dataset_soup, "Overall design"),
        "Contributor(s)": get_next_sibling_text(dataset_soup, "Contributor(s)"),
        "Citation(s)": get_next_sibling_text(dataset_soup, "Citation(s)"),
        "Submission date": get_next_sibling_text(dataset_soup, "Submission date"),
        "Last update date": get_next_sibling_text(dataset_soup, "Last update date"),
        "Contact name": get_next_sibling_text(dataset_soup, "Contact name"),
        "E-mail(s)": get_next_sibling_text(dataset_soup, "E-mail(s)"),
        "Organization name": get_next_sibling_text(dataset_soup, "Organization name"),
        "Department": get_next_sibling_text(dataset_soup, "Department"),
        "Street address": get_next_sibling_text(dataset_soup, "Street address"),
        "City": get_next_sibling_text(dataset_soup, "City"),
        "State/province": get_next_sibling_text(dataset_soup, "State/province"),
        "ZIP/Postal code": get_next_sibling_text(dataset_soup, "ZIP/Postal code"),
        "Country": get_next_sibling_text(dataset_soup, "Country"),
        "Platforms": get_next_sibling_text(dataset_soup, "Platforms"),
        "Samples": get_next_sibling_text(dataset_soup, "Samples"),
        "Bone Scans": get_next_sibling_text(dataset_soup, "Bone Scans"),
        "PET/CT & MRI Findings": get_next_sibling_text(dataset_soup, "PET/CT & MRI Findings"),
        "MicroRNAs & Exosomes": get_next_sibling_text(dataset_soup, "MicroRNAs & Exosomes"),
        "CTCs & Cell-Free DNA (cfDNA)": get_next_sibling_text(dataset_soup, "CTCs & Cell-Free DNA (cfDNA)"),
        "Neoadjuvant Therapy Impact": get_next_sibling_text(dataset_soup, "Neoadjuvant Therapy Impact"),
        "Surgical Margins": get_next_sibling_text(dataset_soup, "Surgical Margins"),
        "Response to Therapy": get_next_sibling_text(dataset_soup, "Response to Therapy"),
        "Organotropism": get_next_sibling_text(dataset_soup, "Organotropism"),
        "Sentinel Lymph Node Status": get_next_sibling_text(dataset_soup, "Sentinel Lymph Node Status"),
        "Lymphovascular Invasion (LVI)": get_next_sibling_text(dataset_soup, "Lymphovascular Invasion (LVI)"),
        "Extracellular Matrix (ECM) Remodeling": get_next_sibling_text(dataset_soup, "Extracellular Matrix (ECM) Remodeling"),
        "Immune Infiltration": get_next_sibling_text(dataset_soup, "Immune Infiltration"),
        "Hypoxia & Angiogenesis": get_next_sibling_text(dataset_soup, "Hypoxia & Angiogenesis"),
        "Extracellular Vesicles (EVs)": get_next_sibling_text(dataset_soup, "Extracellular Vesicles (EVs)"),
        "Circulating Tumor Cells (CTCs)": get_next_sibling_text(dataset_soup, "Circulating Tumor Cells (CTCs)"),
        "Epigenetic Changes": get_next_sibling_text(dataset_soup, "Epigenetic Changes"),
        "Mutational Profile": get_next_sibling_text(dataset_soup, "Mutational Profile"),
        "Hormone Receptor Status": get_next_sibling_text(dataset_soup, "Hormone Receptor Status"),
        "Tumor Size": get_next_sibling_text(dataset_soup, "Tumor Size"),
        "Tumor Grade": get_next_sibling_text(dataset_soup, "Tumor Grade"),
        "Tumor Type & Histology": get_next_sibling_text(dataset_soup, "Tumor Type & Histology"),
        "Family History of Cancer": get_next_sibling_text(dataset_soup, "Family History of Cancer"),
        "Medical History": get_next_sibling_text(dataset_soup, "Medical History"),
        "Age & Sex": get_next_sibling_text(dataset_soup, "Age & Sex")
    }

def process_xml_file(xml_file_path):
    """
    Process the XML file and extract additional data, including Sample Number (ID), Tissue, and Cell line.
    :param xml_file_path: Path to the XML file.
    :return: List of dictionaries, each containing data for a single sample.
    """
    samples_data = []  # List to store data for each sample

    try:
        tree = ET.parse(xml_file_path)
        root = tree.getroot()

        # Define the namespace to handle the XML structure
        ns = {'ns': 'http://www.ncbi.nlm.nih.gov/geo/info/MINiML'}

        # Iterate through each sample in the XML file
        for sample in root.findall(".//ns:Sample", ns):  # Use the namespace in the XPath
            sample_data = {
                "Sample Number (ID)": sample.get("iid", "Not Available"),  # Extract the 'iid' attribute
                "Tissue": None,
                "Cell line": None
            }

            # Extract Tissue and Cell line from the Characteristics elements
            for characteristic in sample.findall(".//ns:Characteristics", ns):
                tag = characteristic.get("tag", "").lower()
                if tag == "tissue":
                    sample_data["Tissue"] = characteristic.text.strip() if characteristic.text else "Not Available"
                elif tag == "cell line":
                    sample_data["Cell line"] = characteristic.text.strip() if characteristic.text else "Not Available"

            samples_data.append(sample_data)

    except Exception as e:
        print(f"[ERROR] Failed to process XML file {xml_file_path}: {e}")

    return samples_data

def scrape_geo():
    results = []
    file_handler = FileHandler(output_dir="data")  # Initialize the FileHandler

    for keyword, url in GEO_SEARCH_URLS.items():
        print(f"Searching GEO for: {keyword}")
        page = requests.get(url, verify=False)
        soup = BeautifulSoup(page.content, "html.parser")

        result_links = soup.select(".rslt a[href^='/geo/query/acc.cgi?acc=']")
        for link in result_links:
            # Extract the accession number from the URL
            result_page_url = f"https://www.ncbi.nlm.nih.gov{link['href']}"
            parsed_url = urlparse(result_page_url)
            accession_number = parse_qs(parsed_url.query).get("acc", ["Unknown"])[0]
            print(f"Extracted accession number from URL: {accession_number}")

            # Visit the result page
            result_page = requests.get(result_page_url,verify=False)
            result_soup = BeautifulSoup(result_page.content, "html.parser")

            # Find the "MINiML formatted family file(s)" link
            miniml_link = result_soup.find("a", string="MINiML formatted family file(s)")
            if miniml_link:
                base_url = miniml_link["href"].replace("ftp://", "https://")  # Change protocol to https
                xml_file_url = f"{base_url}{accession_number}_family.xml.tgz"
                print(f"Constructed full URL with https: {xml_file_url}")

                # Download and extract the .xml file
                xml_file_path = file_handler.download_and_extract_from_url(base_url, accession_number)
                if xml_file_path:
                    print(f"Status: XML file for {accession_number} downloaded and saved at {xml_file_path}")
                    # Process the XML file and extract additional data
                    samples_data = process_xml_file(xml_file_path)
                else:
                    print(f"Status: Failed to process XML file for {accession_number}")
                    samples_data = []
            else:
                xml_file_url = "Not Found"
                xml_file_path = "Failed"
                samples_data = []
                print(f"Status: No MINiML link found for {accession_number}")

            # Extract data from the result page
            base_data = {
                "Keyword": keyword,
                "Accession Number": accession_number,
                "Title": get_text_or_none(result_soup, "Title"),
                "Status": get_text_or_none(result_soup, "Status"),
                "Organism": get_text_or_none(result_soup, "Organism"),
                "Experiment type": get_text_or_none(result_soup, "Experiment type"),
                "Summary": get_text_or_none(result_soup, "Summary"),
                "Overall design": get_text_or_none(result_soup, "Overall design"),
                "Contributor(s)": get_contributors(result_soup),
                "Submission date": get_text_or_none(result_soup, "Submission date"),
                "Last update date": get_text_or_none(result_soup, "Last update date"),
                "Contact name": get_text_or_none(result_soup, "Contact name"),
                "Organization name": get_text_or_none(result_soup, "Organization name"),
                "Street address": get_text_or_none(result_soup, "Street address"),
                "City": get_text_or_none(result_soup, "City"),
                "ZIP/Postal code": get_text_or_none(result_soup, "ZIP/Postal code"),
                "Country": get_text_or_none(result_soup, "Country"),
                "Platforms": get_platforms(result_soup),
                "XML File URL": xml_file_url,
                "XML File Path": xml_file_path
            }

            # Add a row for each sample
            for sample in samples_data:
                row = {**base_data, **sample}  # Merge base data with sample-specific data
                results.append(row)

            # Save the results to the Excel file after processing each dataset
            save_to_excel(results)

    return results

def get_text_or_none(soup, label):
    element = soup.find("td", string=label)
    if element:
        sibling = element.find_next_sibling("td")
        return sibling.get_text(strip=True) if sibling else None
    return None

def get_contributors(soup):
    contributors = soup.find("td", string="Contributor(s)")
    if contributors:
        return ", ".join([a.get_text(strip=True) for a in contributors.find_next_sibling("td").find_all("a")])
    return None

def get_platforms(soup):
    platforms = soup.find("td", string="Platforms")
    if platforms:
        return ", ".join([a.get_text(strip=True) for a in platforms.find_next_sibling("td").find_all("a")])
    return None

def get_samples(soup):
    samples = soup.find("td", string="Samples")
    if samples:
        return ", ".join([a.get_text(strip=True) for a in samples.find_next_sibling("td").find_all("a")])
    return None

def scrape_all_databases():
    results = []

    # Scrape GEO database
    results.extend(scrape_geo())

    # Scrape additional databases
    for db_name, db_url in ADDITIONAL_DATABASES.items():
        print(f"Searching {db_name} for datasets")
        # Implement specific scraping logic for each database
        # This is a placeholder for actual scraping logic
        # For example, you might need to handle pagination, login, etc.
        # Here, we just simulate the process
        results.append({
            "Database": db_name,
            "URL": db_url,
            "Title": "Sample Title",
            "Summary": "Sample Summary",
            "Contact": "Sample Contact"
            # Add more fields as needed
        })

    return results

def main():
    all_results = scrape_all_databases()
    save_to_excel(all_results, "final_result.xlsx")

if __name__ == "__main__":
    main()
