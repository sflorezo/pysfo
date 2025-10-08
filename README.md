# pysfo

pysfo is a Python package designed for comprehensive data analysis and streamlined API interactions.

**Development Notice**  
This package is currently in an **unstable development stage**.  
APIs may change, features may break, and backwards compatibility is not guaranteed.  
Use at your own risk.

__________
## Features

- **API Interactions**: Modules to interact with various APIs like FRED, IMF, and others.
- **Data Analysis**: Provides tools for economic data analysis, including exchange rates and financial operations.
- **Batch Jobs**: Automated processes for handling data tasks.
- **Basic Utilities**: Essential functions for data manipulation and time utilities.
- **NLP Tools**: Support in generating code and configurations using LLM based tools.

__________
## File Structure

- **pysfo/pulldata**: Modules for pulling and processing data from various data sources:
  - `cmns.py`: Common utilities for data manipulation.
  - `efa_row.py`: Tools for row-level economic and financial analysis.
  - `exchangerates.py`: Functions to handle and retrieve exchange rate data.
  - `fetch_fred_api.py`: Interfaces and methods for fetching data from the FRED API.
  - `fof.py`: Tools and functions for analyzing flow of funds data.
  - `fomc_dates.py`: Functions related to FOMC meeting dates and related information.
  - `fred.py`: Functions to pull cleaned series from FRED, after downloading them through the FRED API.
  - `imf_ifs.py`: Access and analyze data from the International Monetary Fund's International Financial Statistics.
  - `other.py`: Miscellaneous utilities related to data retrieval and manipulation.
- **pysfo/dbnomicstools**: Tools specific to DBnomics, designed for handling economic datasets efficiently.
- **pysfo/llmtools**: Natural Language Processing tools assisting in code generation and configuration.
- **pysfo/basic**: Contains essential utility functions for data manipulation including:
  - `basicfns.py`: Basic functions for handling data operations.
  - `groupby_apply_various.py`: Utilities for group-by operations and applying functions.
  - `regfns.py`: Functions for regression analysis.
  - `time_utils.py`: Tools for handling and manipulating time-related data.
- **pysfo/batch_jobs**: Contains scripts for automating batch data processing tasks. It manages repetitive data jobs to improve efficiency and accuracy.
- **tests**: Comprehensive unit tests for core functionalities ensuring robustness and reliability of the package.

__________
## Installation

Install directly from the GitHub repository:

```bash
pip install git+https://github.com/sflorezo/pysfo.git
```

__________
## Usage

Example for importing and using the package, that imports the cleaned Enhanced Financial Accounts of the USA (after downloading main data manually):

```python
import pysfo
from pysfo.pulldata import efa_row

# Example to fetch data using FRED API
data = efa_row.get_efa_row(efa_row_dir, level = "agg")

# ... additional usage examples ...
```

__________
## Requirements

All Python dependencies are listed in `requirements.txt`. Ensure you have the necessary libraries installed:

```bash
pip install -r requirements.txt
```

__________
## Testing

Run all tests using pytest to ensure functionality:

```bash
pytest tests/
```

__________
## Contribution

This is currently a personal project, and external contributions are not being accepted at this time. Thank you for your interest!

__________
## License

This project is licensed under the MIT License - see the LICENSE file for details.

__________
## Contact

For questions or issues, please contact Sergio Florez-Orrego at [saf9215@stern.nyu.edu](mailto:saf9215@stern.nyu.edu).
