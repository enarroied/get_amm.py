[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)
![License](https://img.shields.io/badge/License-MIT-blue.svg)
![Python Version](https://img.shields.io/badge/Python-3.8%2B-blue.svg)

# get_amm.py

## Introduction

This small script downloads and cleans data from the French Open Data catalog of phytopharmaceutical products (AKA pesticides).
The script selects **organic** pesticides allowed for the use in **vines** against major diseases : mildew and powdery mildew, as well as major pests : grape moth and leafhoppers.


## Why I created this script - please read before using it

I created this script to fill a sheet from a pre-existing Excel file that allows organic winegrowers to record their vine sprayings.
The Excel file uses the data that this script generates. Since this is regulatory information, it's important to keep the file data up to date, at least once a year. 

Before this script, the team manually updated the data by hand checking every product from a official website. That task took over a day and was a great source of invalid values.

The function returns the data in the same format as the excel sheet, there is no other reason for the table format.

Separators in the CSV files are ";" semicolons since data is in French format with "," commas as decimal separators.

## Legal Disclaimer

**Caution:**
- The data provided by this project may not always be up to date, even if it comes from official Open Data.
- I created the files and the program without documentation from the source data, relying on domain knowledge and combining it with Python expertise. It may not cover some unusual conditions. 
- The data may change in the future, introducing cases that did not exist when I created the program and subsequently refactored it.

## Data Source

The data comes from the [Open data from the E-Phy catalog of plant protection products, fertilizing materials, growing media, adjuvants, mixed products, and blends.](https://www.data.gouv.fr/fr/datasets/donnees-ouvertes-du-catalogue-e-phy-des-produits-phytopharmaceutiques-matieres-fertilisantes-et-supports-de-culture-adjuvants-produits-mixtes-et-melanges/)

## Function documentation

You can check the [API documentation](https://enarroied.github.io/get_amm.py/get_amm.html). it's generated automatically with [pdoc](https://pdoc.dev/docs/pdoc.html).

## I refactored the code
I refactored this code at the end of 2023. The original code was hard to read and maintain. This is one my first projects, I did not know pandas, so I read the files, line by line, and I created the new file iteratively. 
## Installation
1. Clone the repository to your local machine:
    ```bash
    git clone https://github.com/your-username/your-repository.git
    ```
2. Navigate to the project directory:
    ```bash
    cd your-repository
    ```
3. Create a virtual environment (optional but recommended):
    ```bash
    python -m venv venv
    ```
4. Activate the virtual environment:
    ```bash
    source venv/bin/activate  # On Unix or MacOS
    # OR
    venv\Scripts\activate  # On Windows
    ```
5. Install dependencies:
    ```bash
    pip install -r requirements.txt
    ```


**You can also just download the function and call it from a notebook**. The script uses common Python libraries:
* Numpy
* Pandas
* requests
* BeautifulSoup

## Usage

You can see an example how to call the function in the demo directory. You can find an output file as well.