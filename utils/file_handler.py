import os
import requests
import pandas as pd
from bs4 import BeautifulSoup
from tqdm import tqdm
import tarfile

class FileHandler:
    def __init__(self, output_dir="data"):
        self.output_dir = output_dir
        self.xml_dir = os.path.join(self.output_dir, "xml")
        os.makedirs(self.output_dir, exist_ok=True)
        os.makedirs(self.xml_dir, exist_ok=True)
        self.excel_file = os.path.join(self.output_dir, "final_result.xlsx")

        # Define the columns for the Excel file
        self.columns = [
            "Dataset URL", "Keyword", "Title", "Summary", "Experiment type", "Overall design",
            "Contributor(s)", "Citation(s)", "Submission date", "Last update date", "Contact name",
            "E-mail(s)", "Organization name", "Department", "Street address", "City", "State/province",
            "ZIP/Postal code", "Country", "Platforms", "Samples"
        ]

        # Initialize the Excel file if it doesn't exist
        if not os.path.exists(self.excel_file):
            df = pd.DataFrame(columns=self.columns)
            df.to_excel(self.excel_file, index=False, engine='openpyxl')

    def append_to_excel(self, data):
        """
        Append a row of data to the Excel file.
        :param data: Dictionary containing data for each column.
        """
        df = pd.read_excel(self.excel_file, engine='openpyxl')
        df = pd.concat([df, pd.DataFrame([data])], ignore_index=True)
        df.to_excel(self.excel_file, index=False, engine='openpyxl')
        print(f"[INFO] Data appended to {self.excel_file}")

    def download_xml(self, url, accession):
        """
        Download and save the XML file for the given accession.
        :param url: Base URL for XML download.
        :param accession: Accession ID for the dataset.
        """
        xml_url = f"{url}&form=xml&view=full"
        try:
            response = requests.get(xml_url)
            response.raise_for_status()
            xml_path = os.path.join(self.xml_dir, f"{accession}.xml")
            with open(xml_path, "wb") as file:
                file.write(response.content)
            print(f"[INFO] XML file saved: {xml_path}")
        except Exception as e:
            print(f"[ERROR] Failed to download XML for {accession}: {e}")

    def download_and_extract_from_url(self, base_url, accession_number):
        """
        Download and extract the XML file from the given URL, skipping files larger than 10 MB.
        :param base_url: Base URL for the file.
        :param accession_number: Accession number for the dataset.
        :return: Path to the extracted XML file or None if skipped or failed.
        """
        # Construct the full URL
        file_name = f"{accession_number}_family.xml.tgz"
        full_url = f"{base_url}{file_name}"  # Append the file name to the base URL
        tgz_file_path = os.path.join(self.output_dir, file_name)  # Full path to the .tgz file
        extracted_file_path = os.path.join(self.xml_dir, f"{accession_number}_family.xml")  # Save in xml folder

        # Debugging: Print the constructed URL and paths
        print(f"Constructed full URL: {full_url}")
        print(f"tgz_file_path: {tgz_file_path}")
        print(f"extracted_file_path: {extracted_file_path}")

        try:
            # Check the file size before downloading
            print(f"Checking file size for: {full_url}")
            response = requests.head(full_url)
            response.raise_for_status()
            file_size = int(response.headers.get('content-length', 0))  # Get the file size in bytes

            # Convert file size to MB and check if it exceeds 10 MB
            if file_size > 30 * 1024 * 1024:  # 10 MB in bytes
                print(f"[INFO] Skipping {file_name} as it exceeds 10 MB (size: {file_size / (1024 * 1024):.2f} MB)")
                return None

            # Download the .tgz file with a progress bar
            print(f"Downloading: {full_url}")
            response = requests.get(full_url, stream=True)
            response.raise_for_status()
            total_size = int(response.headers.get('content-length', 0))
            
            with open(tgz_file_path, "wb") as f, tqdm(
                desc=f"Downloading {file_name}",
                total=total_size,
                unit="B",
                unit_scale=True,
                unit_divisor=1024,
            ) as bar:
                for chunk in response.iter_content(chunk_size=1024):
                    f.write(chunk)
                    bar.update(len(chunk))

            # Extract the .xml file
            print(f"Extracting: {tgz_file_path}")
            with tarfile.open(tgz_file_path, "r:gz") as tar:
                for member in tar.getmembers():
                    if member.name.endswith(".xml"):
                        member.name = os.path.basename(member.name)  # Remove directory structure
                        extracted_path = os.path.join(self.xml_dir, member.name)
                        if not os.path.exists(extracted_path):  # Avoid overwriting existing files
                            tar.extract(member, self.xml_dir)
                            print(f"Extracted: {member.name}")
                        else:
                            print(f"File already exists: {extracted_path}")
                        return extracted_file_path

        except Exception as e:
            print(f"Error processing {full_url}: {e}")
            return None

        finally:
            # Clean up the .tgz file
            print(f"Cleaning up: {tgz_file_path}")
            if os.path.exists(tgz_file_path) and os.path.isfile(tgz_file_path):
                os.remove(tgz_file_path)

    def scrape_and_extract_data(self, url):
        """
        Scrape data from the HTML page and insert it as a row in the Excel file.
        :param url: URL of the HTML page to scrape.
        """
        try:
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
                "Title": get_text_or_none("Title"),
                "Summary": get_text_or_none("Summary"),
                "Experiment type": get_text_or_none("Experiment type"),
                "Overall design": get_text_or_none("Overall design"),
                "Contributor(s)": ", ".join(
                    [a.get_text(strip=True) for a in soup.find("td", string="Contributor(s)").find_next_sibling("td").find_all("a")]
                ) if soup.find("td", string="Contributor(s)") else None,
                "Citation(s)": get_text_or_none("Citation(s)"),
                "Submission date": get_text_or_none("Submission date"),
                "Last update date": get_text_or_none("Last update date"),
                "Contact name": get_text_or_none("Contact name"),
                "E-mail(s)": get_text_or_none("E-mail(s)"),
                "Organization name": get_text_or_none("Organization name"),
                "Department": get_text_or_none("Department"),
                "Street address": get_text_or_none("Street address"),
                "City": get_text_or_none("City"),
                "State/province": get_text_or_none("State/province"),
                "ZIP/Postal code": get_text_or_none("ZIP/Postal code"),
                "Country": get_text_or_none("Country"),
                "Platforms": ", ".join(
                    [a.get_text(strip=True) for a in soup.find("td", string="Platforms").find_next_sibling("td").find_all("a")]
                ) if soup.find("td", string="Platforms") else None,
                "Samples": ", ".join(
                    [a.get_text(strip=True) for a in soup.find("td", string="Samples").find_next_sibling("td").find_all("a")]
                ) if soup.find("td", string="Samples") else None,
            }

            # Append the extracted data as a row in the Excel file
            self.append_to_excel(data)

        except Exception as e:
            # Log the error without changing the terminal output format
            print(f"[ERROR] Error scraping data from {url}: {e}")