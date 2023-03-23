import io, os, requests, threading, time, zipfile
from typing import Any
from functools import cache
import pandas as pd

max_simultaneous_downloads = 2
download_semaphore = threading.Semaphore(max_simultaneous_downloads)

class NhgisApi():
    def __init__(self, api_key: str|None = None):
        if api_key is None:
            api_key = open("secrets/ipums_api_key.txt", "r").read()
        self.api_key = api_key

    def headers(self):
        return {"Authorization": self.api_key}

    @cache
    def get_shapefiles_metadata(self) -> pd.DataFrame:
        url = "https://api.ipums.org/metadata/nhgis/shapefiles?version=2"
        records = []
        while True:
            metadata_page = requests.get(url, headers=self.headers()).json()
            shapefiles = metadata_page["data"]
            print(f"Received {len(shapefiles)} shapefile metadata records from {url}")
            records += shapefiles
            next_url = metadata_page["links"]["nextPage"]
            if next_url is None:
                break
            # Workaround for bug in API
            url = f'{url.split("?")[0]}?{next_url.split("?")[1]}'
        level_map = {
            "Block": "block",
            "Block Group": "blockgroup",
            "Census Tract": "tract",
            "Place": "place",
            "County": "county",
            "State": "state"
        }
        for record in records:
            record["geographic_level_id"] = level_map.get(record["geographicLevel"], record["geographicLevel"])
            record["basis_id"] = record["name"].split("_")[-1]

        ret = pd.DataFrame(records)
        return ret
    
    def request_extract(self, shapefile_names: list[str]):
        url = "https://api.ipums.org/extracts?collection=nhgis&version=2"
        descriptions: list[str] = []
        if len(shapefile_names) == 1:
            descriptions.append(f"shapefile {shapefile_names[0]}")
        elif len(shapefile_names) > 1:
            descriptions.append(f"shapefiles {shapefile_names[0]} ... {shapefile_names[-1]}")
        description = "; ".join(descriptions)
        body: dict[str, Any] = {
            "datasets": {},
            "timeSeriesTables": {},
            "shapefiles": shapefile_names,
            "timeSeriesTableLayout": "time_by_file_layout",
            "dataFormat": "csv_header",
            "description": description,
            "breakdownAndDataTypeLayout": "single_file"
        }
        result = requests.post(url, headers=self.headers(), json=body)
        try:
            extract_number = result.json()["number"]
        except:
            print(result.status_code, result.text)
            raise Exception("Failed to create extract")
        print(f'  Extract {extract_number} created: "{description}"')
        return extract_number
    
    def wait_for_extract(self, extract_number: int):
        # Wait for extract to complete and get info
        message_shown = False
        while True:
            r = requests.get(
                f"https://api.ipums.org/extracts/{extract_number}?collection=nhgis&version=2",
                headers=self.headers()
            )
            if r.status_code != 200:
                print(r.status_code, r.text)
                r.raise_for_status()
            extract_info = r.json()
            status = extract_info["status"]
            if status == "completed":
                return extract_info
            elif status in ["failed", "canceled"]:
                raise Exception(f"Extract {extract_number} status {status}")
            if not message_shown:
                print(f"  Extract {extract_number}: waiting to complete")
                message_shown = True
            time.sleep(5)

    def download_extract(self, extract_number: int, output_dir: str):
        extract_info = self.wait_for_extract(extract_number)        
        # Download extract files
        print(f"  Extract {extract_number}: downloading to {output_dir}")
        tmp_output_path =  f"{output_dir}.tmp.{os.getpid()}"
        os.makedirs(tmp_output_path, exist_ok=True)
        download_links = extract_info["downloadLinks"]
        if "gisData" in download_links:
            shapefiles_path = f"{tmp_output_path}/shapefiles"
            os.mkdir(shapefiles_path)

            with download_semaphore:
                content = requests.get(download_links["gisData"]["url"], headers=self.headers()).content
            file_length = len(content)
            expected_length = download_links["gisData"]["bytes"]
            assert(file_length == expected_length), f"Downloaded file length {file_length} does not match expected length {expected_length}"

            gisdata_zip = zipfile.ZipFile(io.BytesIO(content))
            for shapefile_zip in gisdata_zip.namelist():
                subdir = f"{shapefiles_path}/{os.path.splitext(os.path.basename(shapefile_zip))[0]}"
                shapefile_zip = zipfile.ZipFile(gisdata_zip.open(shapefile_zip))
                shapefile_zip.extractall(subdir)

            print(f"  Extract {extract_number}: downloaded {len(os.listdir(shapefiles_path))} shapefiles to {shapefiles_path}")
        os.rename(tmp_output_path, output_dir)
        print(f"  Extract {extract_number}: Completed {output_dir}")

    
    # def find_shapefile_names_for_year_level(self, year, geographicLevel):
    #     all = self.get_shapefiles_metadata().query(f"year == '{year}' and geographicLevel == '{geographicLevel}'")
    #     # If multiple options, pick the "max" name, which should be the most source year
    #     constituent_names = list(all.groupby("extent")["name"].max())

    #     if geographicLevel == "block":
    #         assert len(constituent_names) > 50, f"Expected >50 constituents, got {len(constituent_names)}"
    #         constituents = [{"fips": name[:2], "name": name} for name in constituent_names]
    #         table_name = re.sub(r"^\d+_", "", constituent_names[0])
    #     else:
    #         assert len(constituent_names) == 1, f"Expected 1 constituent, got {len(constituent_names)}"
    #         constituents = [{"fips": "us", "name": constituent_names[0]}]
    #         table_name = constituent_names[0]

    #     return {
    #         "table_name": table_name,
    #         "constituents": constituents
    #     }