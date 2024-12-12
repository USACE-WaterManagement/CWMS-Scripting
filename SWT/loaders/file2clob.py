import os
import asyncio
import aiohttp
from aiohttp import ClientSession


# Add these to your .bashrc or windows environment variables
# Make sure they match exactly
CDA_KEY = os.getenv("CDA_API_KEY") # Generated in Swagger after a /CWMSLogin/login
OFFICE = os.getenv("OFFICE") # i.e. SWT
REPORT_DIRECTORY = os.getenv("REPORT_PATH", r"G:\\wcds\\RESCON\\Charts\\bin")

if not CDA_KEY:
    print("CDA_API_KEY environment variable not set.")
    exit(1)
if not OFFICE:
    print("OFFICE environment variable not set.")
    exit(1)

class Report:
    '''
        Report class to handle reading and writing reports to CDA via the CLOB endpoint
    '''
    def __init__(self, filename):
        self.filename = os.path.split(filename)[1]
        self.file_path = filename
        self.text = None
        self.found = os.path.exists(filename)
        self.full_year = None

    def open(self):
        '''
            Open the file and read the contents into memory
        '''
        if self.found:
            with open(self.file_path, 'r') as f:
                self.text = f.read()
        return self.text

    def _convertFilenameYear(self):
        '''
            Convert the filename year to a 4 digit year
        '''
        name, ext = os.path.splitext(self.filename.upper())
        year = int(name[-2:])
        rem_name = name[:-2]
        if year > 50:
            year = f"19{year}"
        else:
            year = f"20{year}"
        self.full_year = year
        return rem_name + year + ext

    async def writeCDA(self, session: ClientSession, fail_if_exists=False):
        '''
            Write the report to CDA via the CLOB endpoint
        '''
        try:
            if not self.text:
                self.open()
            if not self.full_year:
                self._convertFilenameYear()
            if self.found:
                headers = {
                    "Content-Type": "application/json;version=2",
                    "Authorization": f"apikey {CDA_KEY}",
                    "accept": "*/*"
                }
                payload = {
                    "office-id": OFFICE,
                    "id": self._convertFilenameYear(),
                    "description": self.text.split("\n")[0].strip(),
                    "value": self.text
                }
                async with session.post(
                    f"https://wm.swt.ds.usace.army.mil:8243/swt-data/clobs?fail-if-exists={fail_if_exists}",
                    headers=headers,
                    json=payload,
                    ssl=False
                ) as response:
                    if response.status == 201:
                        print(f"Successfully wrote report to CDA ({self.filename}).")
                        print(f"Access report here: https://wm.swt.ds.usace.army.mil:8243/swt-data/clobs/{self.filename.upper()}?office={OFFICE}")
                    else:
                        print(f"Failed to write report to CDA ({self.filename}). {response.status}: {await response.text()}")
            else:
                print(f"Failed to write report to CDA ({self.filename}). Either the report file was not found on disk, no text in the file, or the file has not been opened yet.")
        except Exception as e:
            print(f"Failed to write report to CDA ({self.filename}). {e}")

def read_reports(directory):
    '''
        Read all reports in the directory ending in .txt
    '''
    for filename in os.listdir(directory):
        if filename.endswith('.txt'):
            yield Report(os.path.join(directory, filename))

async def process_reports(reports):
    '''
        Process all reports in the list via async call, 12 at a time
    '''
    async with aiohttp.ClientSession() as session:
        tasks = []
        for report in reports:
            if report.found:
                if not report.filename.find("24") >= 0:
                    continue
                print("Loading report: " + report.filename)
                tasks.append(report.writeCDA(session))
            else:
                print("Report not found: " + report.filename)
        # Limit concurrency to 12 tasks at a time
        semaphore = asyncio.Semaphore(12)

        async def limited_task(task):
            async with semaphore:
                await task

        await asyncio.gather(*(limited_task(task) for task in tasks))

def main():
    print("loading reports into cda...")
    reports = list(read_reports(REPORT_DIRECTORY))
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        loop.run_until_complete(process_reports(reports))
    finally:
        loop.run_until_complete(loop.shutdown_asyncgens())
        loop.close()

if __name__ == "__main__":
    main()