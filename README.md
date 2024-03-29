# DeID Anonymizer

DeID Anonymizer is a simple graphical tool to anonymize datasets using k-anonymity. The application is built using Python, PyQt5 for the user interface, and PySpark for processing the datasets.

## Features

- Supports loading and saving datasets in CSV and JSON formats
- Allows users to choose which columns to anonymize and set an interval size for generalization
- Performs k-anonymity on the selected columns
- Hashes string columns to protect sensitive information
- Provides a simple and user-friendly interface

## Demo

[Demo-Ubuntu](https://user-images.githubusercontent.com/55834722/234714836-3ba34f71-9473-4a10-a481-3bcf45b63b40.webm)


## Installation

### Prerequisites

Ensure you have the following installed:

- Python 3.6 or later
- Git

### Cloning the Repository

Clone the repository using the following command:
```bash
git clone https://github.com/kuladeepmantri/deid-anonymizer.git
```
### Installing Dependencies

1. Navigate to the project folder:
```bash
cd DeID-Anonymizer
```

2. Install the required packages using the following command:
```bash
pip install -r requirements.txt
```

## Usage

1. Run the application:
```bash
python DeID.py
```

2. Use the graphical interface to load your dataset, choose the columns to anonymize, set k value, and save the anonymized dataset.

## Contributing

Pull requests are welcome. For major changes, please open an issue first to discuss what you would like to change.

## License

[MIT](https://choosealicense.com/licenses/mit/)




