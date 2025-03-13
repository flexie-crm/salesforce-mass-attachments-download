# Salesforce Mass Attachments Download

A Python script to **efficiently download Salesforce attachment files** in bulk. This script is designed to handle large volumes of files while keeping track of progress, allowing recovery in case of failure. A `run` file is included to automatically install the required environment and dependencies.

## Features

- **Mass Download**: Fetch and download large numbers of Salesforce attachments.
- **Resumable**: Keeps track of downloaded files and resumes from where it left off if interrupted.
- **Automatic Setup & Execution**: A `run` script is included to install dependencies, set up the environment, and execute the script.
- **Error Handling**: Automatically retries failed downloads and logs errors.
- **Multi-threaded Downloads**: Utilizes multi-threading (`ThreadPoolExecutor`) for faster downloads.
- **Exponential Backoff**: Implements retry logic with increasing delay to handle API rate limits.

## Requirements

- Python 3.x
- Salesforce API credentials (`username`, `password`, `security_token`)
- `pip` for dependency management

## Installation & Usage

Clone this repository and navigate to the directory:

```sh
git clone https://github.com/flexie-crm/salesforce-mass-attachments-download.git
cd salesforce-mass-attachments-download
```

### Run the script

Simply execute:

```sh
chmod +x run
./run
```

This will:
- Create a virtual environment
- Install necessary dependencies
- Authenticate with Salesforce
- Fetch and download attachments while keeping track of progress

## Configuration

1. **Salesforce API Credentials:** Modify the script with your credentials:
   ```python
   USERNAME = "your_username"
   PASSWORD = "your_password"
   SECURITY_TOKEN = "your_security_token"
   ```
2. **Logging:** Errors and status updates are logged in `download_errors.log`.
3. **Configuration Parameters:**
   - `MAX_THREADS = 10` → Number of parallel downloads.
   - `RETRY_COUNT = 5` → Number of retry attempts for failed downloads.
   - `SALESFORCE_API_VERSION = "62.0"` → API version.
   - `BATCH_LIMIT = "200"` → Maximum records per request (up to 2000 allowed by Salesforce).

## Logging & Recovery

- **Resumable Downloads**: Progress is stored in `last_marker.json`, allowing recovery from failures.
- **Log Files**:
  - `download_errors.log`: Tracks failed downloads.
  - `attachments_metadata.csv`: Stores metadata of downloaded attachments.

## License

This project is licensed under the MIT License. See the `LICENSE` file for details.

## Contributions

Contributions, issues, and feature requests are welcome! Feel free to submit a pull request.

## Contact

For any inquiries, please contact **Flexie CRM** or open an issue in this repository.

---
Happy downloading! 🚀