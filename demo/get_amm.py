import re
from io import BytesIO
from zipfile import ZipFile

import numpy as np
import pandas as pd
import requests
from bs4 import BeautifulSoup


class URLNotFoundError(Exception):
    pass


class FileNameNotFoundError(Exception):
    pass


def get_url():
    """
    Retrieves the URL used to download the file with the amm information. It' the one
    ending with '-utf8.zip' from the specified root URL (government Open Data).

    Returns:
        str or int: If a matching URL is found, returns the URL as a string.
    If no matching URL is found, raise URLNotFoundError.
    """

    root_url = "https://www.data.gouv.fr/fr/datasets/donnees-ouvertes-du-catalogue-e-phy-des-produits-phytopharmaceutiques-matieres-fertilisantes-et-supports-de-culture-adjuvants-produits-mixtes-et-melanges/"
    root_html = requests.get(root_url)

    soup_amm = BeautifulSoup(root_html.text, features="lxml")

    anchor_tags = soup_amm.find_all("a")

    for tag in anchor_tags:
        href = tag.get("href")
        if href:
            if "-utf8.zip" in href:
                return href

    raise URLNotFoundError("URL ending with '-utf8.zip' not found on the page.")


def download_and_extract_dataframe(
    amm_url, file_name="usages_des_produits_autorises_utf8.csv"
):
    """
    Downloads a zip file from the URL with AMM data and extracts its CSV contents into a Pandas DataFrame.

    Args:
        amm_url (str): The URL of the zip file to download and extract.
        file_name (str): The name of the CSV file to extract from the zip archive.

    Returns:
        pd.DataFrame: A pandas DataFrame containing the CSV content.
    """
    response = requests.get(amm_url)

    with ZipFile(BytesIO(response.content), "r") as zip_file:
        file_list = zip_file.namelist()

        if file_name not in file_list:
            raise FileNameNotFoundError(
                f"File '{file_name}' not found in the zip archive."
            )

        with zip_file.open(file_name) as csv_file:
            # FRENCH format --> separator is ";", set encoding too
            df = pd.read_csv(csv_file, sep=";", encoding="utf-8")
            return df


def clean_df_amm(df_amm):
    """
    Cleans the DataFrame of agricultural product usages (df_amm). It returns rows for
        products allowed in Organic Farming ("Agriculture Biologique"), it excludes
        marginal uses (Thrips, Esca...). Note that it excludes Black Rot as well, which could
        be problematic (functionally). It return products that are still allowed.

    Args:
        df_amm (pd.DataFrame): The DataFrame containing agricultural product usage data.

    Returns:
        pd.DataFrame: A cleaned DataFrame containing rows that meet the specified criteria.

    Technical riteria:
    1. Select rows where the column "mentions autorisees" contains the term.
    "Utilisable en agriculture biologique" (case-insensitive).
    2. Further filter the DataFrame to rows where the column "identifiant usage".
    contains the term "vigne" (case-insensitive).
    3. Exclude rows where the column "mentions autorisees" contains any of the terms.
    in the list of "exclude_usages" (case-insensitive).
    4. Select rows where the column "etat usage" contains the term "Retrait" (case-insensitive).
    """
    df_bio = df_amm[
        df_amm["mentions autorisees"].str.contains(
            "agriculture biologique|production biologique", case=False, na=False
        )
    ]

    df_bio_vigne = df_bio[
        df_bio["identifiant usage"].str.contains("vigne", case=False, na=False)
    ]
    exclude_usages = [
        "Thrips",
        "Black rot",
        "Bactérioses",
        "Excoriose",
        "Erinose",
        "Cochenilles",
        "Aleurodes",
        "Pourriture grise",
        "Mouches",
        "Stad. Hivern. Ravageurs",
        "lack dead arm",
        "Esca",
        "Chenilles phytophages",
        "Eutypiose",
        "Acariens",
    ]

    df_bio_vigne_main = df_bio_vigne[
        ~df_bio_vigne["mentions autorisees"].str.contains(
            "|".join(exclude_usages), case=False, na=False
        )
    ]

    df_bio_vigne_main_authorised = df_bio_vigne_main[
        df_bio_vigne_main["etat usage"].str.contains("Autorisé", na=False)
    ]

    df_bio_vigne_main_authorised = df_bio_vigne_main_authorised.reset_index()

    return df_bio_vigne_main_authorised


def add_others(df_bio_vigne_main_authorised):
    """
    Adds a new column 'Autres' to the DataFrame based on conditions in 'condition emploi' and 'gamme usage'.

    Args:
        df_bio_vigne_main_authorised (pd.DataFrame): The DataFrame containing data.

    Returns:
        pd.DataFrame: The DataFrame with the new 'Autres' column added.

    Conditions:
    * If 'badigeon' is present in 'condition emploi', 'Autres' will contain 'badigeon'.
    * If 'jardin' is present in 'gamme usage', 'Autres' will contain 'Jardin autorisé'.
    * If both conditions are met, 'Autres' will contain 'badigeon|Jardin autorisé', separated by a pipe (|) character.
    """

    # Function to merge two columns with a pipe separator
    def merge_columns(row):
        result = ""

        # Check for 'badigeon' in 'condition emploi'
        if pd.notna(row["condition emploi"]) and "badigeon" in row["condition emploi"]:
            result += "badigeon"

        # Check for 'jardin' in 'gamme usage'
        if pd.notna(row["gamme usage"]) and "jardin" in row["gamme usage"]:
            result += "|" if result else ""
            result += "Jardin autorisé"

        return result

    df_bio_vigne_main_authorised["Autres"] = df_bio_vigne_main_authorised.apply(
        merge_columns, axis=1
    )

    return df_bio_vigne_main_authorised


def get_active_compound(df_bio_vigne_main_authorised_with_others):
    """
    Extracts the active compound from the 'Substances actives' column and creates a new column 'Active Compound'.
    The active compound in in French ("matière active").

    Args:
        df_bio_vigne_main_authorised_with_others (pd.DataFrame): The DataFrame containing information about pesticides.

    Returns:
        pd.DataFrame: The DataFrame with the new 'Active Compound' column added.

    Operation:
    The function extracts the active compound from the 'Substances actives' column, which typically includes
    information about the active compound and its concentration. It creates a new column 'Active Compound'
    containing only the active compound name.
    """

    df_bio_vigne_main_authorised_with_others["Active Compound"] = (
        df_bio_vigne_main_authorised_with_others["Substances actives"]
        .str.split("(")
        .str[0]
    )
    return df_bio_vigne_main_authorised_with_others


def process_concentration(value):
    """
    Extracts and processes the concentration value from the 'Substances actives' column.

    Args:
        value (str): The value from the 'Substances actives' column.

    Returns:
        float or np.nan: The processed concentration value as a float, rounded to two decimal places,
            or np.nan if an error occurs during processing.

    Operation:
    * The function extracts numeric characters from the input value using a regular expression.
    * It checks if the value contains a percent sign ("%") or " g/". Depending on the case,
    it removes non-numeric characters, converts the value to a float, and rounds it accordingly.
    * If any error occurs during processing, it returns np.nan.
    """

    def remove_non_numbers(input_string):
        # Use a regular expression to match only digits and points
        cleaned_string = re.sub(r"[^0-9.]", "", input_string)
        return cleaned_string

    try:
        concentration_number = "".join([char for char in value if char.isdigit()])

        if "%" in value:
            clean_concentration_number = remove_non_numbers(concentration_number)
            concentration = round(float(clean_concentration_number) / 10, 2)
        elif " g/" in value:
            clean_concentration_number = remove_non_numbers(concentration_number)
            concentration = round(float(clean_concentration_number) / 100, 2)
        else:
            clean_concentration_number = remove_non_numbers(concentration_number)
            concentration = round(float(clean_concentration_number) / 100, 2)

        return concentration

    except:
        return np.nan


def make_second_names_main(df_bio_vigne_main_authorised_with_others_compounds):
    """
    Expand the DataFrame by creating new rows for each product name in the 'seconds noms commerciaux' column.
    Each new line has the second name as the main name ("nom produit").
    WARNING: this uses iterrows(). It should be fine since the volume of data is low.

    Args:
        df_bio_vigne_main_authorised_with_others_compounds (DataFrame): The input DataFrame.

    Returns:
        DataFrame: A new DataFrame with additional rows for each product name in the 'seconds noms commerciaux' column.
    """
    column_names = df_bio_vigne_main_authorised_with_others_compounds.columns.tolist()
    df_second_name = pd.DataFrame(columns=column_names)
    for _, row in df_bio_vigne_main_authorised_with_others_compounds.iterrows():
        second_names_raw = row["seconds noms commerciaux"]
        if pd.notna(second_names_raw):
            second_names_list = second_names_raw.split("|")
            for second_name in second_names_list:
                second_name = second_name.strip()

                second_row = row.copy()
                second_row["nom produit"] = second_name

                df_second_name = pd.concat(
                    [df_second_name, second_row.to_frame().T], ignore_index=True
                )

    df_bio_vigne_main_authorised_with_others_compounds = pd.concat(
        [df_bio_vigne_main_authorised_with_others_compounds, df_second_name],
        ignore_index=True,
    )

    return df_bio_vigne_main_authorised_with_others_compounds


def create_df_cuivre(df_bio_vigne_main_authorised_with_others_compounds):
    """
    Create a DataFrame specific to copper compounds 'cuivre' containing relevant information.
    The format is the one for the final excel file.

    Args:
        df_bio_vigne_main_authorised_with_others_compounds (pd.DataFrame): Original DataFrame.

    Returns:
        pd.DataFrame: DataFrame filtered for 'cuivre' with additional calculated columns.
            Columns are; 'nom produit', 'Active Compound', 'Autres', 'dose retenue', 'Dose',
            and 'nombre max d'application'.
    """
    df_cuivre = df_bio_vigne_main_authorised_with_others_compounds[
        df_bio_vigne_main_authorised_with_others_compounds[
            "Active Compound"
        ].str.contains("cuivre", case=False, na=False)
    ].reset_index(drop=True)

    df_cuivre["Concentration"] = df_cuivre["Substances actives"].str.split(")").str[1]

    df_cuivre["Concentration"] = df_cuivre["Concentration"].apply(process_concentration)
    df_cuivre["Dose"] = (
        ((df_cuivre["Concentration"] / 100) * df_cuivre["dose retenue"])
        .astype(float)
        .round(1)
    )
    df_cuivre = df_cuivre[
        [
            "nom produit",
            "Active Compound",
            "Autres",
            "Concentration",
            "dose retenue",
            "Dose",
            "nombre max d'application",
        ]
    ]

    return df_cuivre


def create_df_soufre(df_bio_vigne_main_authorised_with_others_compounds):
    """
    Create a DataFrame specific to sulphur products 'soufre' containing relevant information.
    The format is the one for the final excel file.

    Args:
        df_bio_vigne_main_authorised_with_others_compounds (pd.DataFrame): Original DataFrame.

    Returns:
        pd.DataFrame: DataFrame filtered for 'soufre' with additional calculated columns.
            Columns are; 'nom produit', 'Active Compound', 'Autres', 'dose retenue',  "Biocontrôle (1/0)", 'Dose',
            and 'nombre max d'application'.
    """
    df_soufre = df_bio_vigne_main_authorised_with_others_compounds[
        df_bio_vigne_main_authorised_with_others_compounds[
            "Active Compound"
        ].str.contains("soufre|sulphur", case=False, na=False)
    ].reset_index(drop=True)

    df_soufre["Concentration"] = df_soufre["Substances actives"].str.split(")").str[1]

    df_soufre["Concentration"] = df_soufre["Concentration"].apply(process_concentration)
    df_soufre["Dose"] = (
        ((df_soufre["Concentration"] / 100) * df_soufre["dose retenue"])
        .astype(float)
        .round(1)
    )

    df_soufre["Biocontrôle (1/0)"] = df_soufre["mentions autorisees"].apply(
        lambda x: 1 if "biocontrôle" in x.lower() else 0
    )
    df_soufre = df_soufre[
        [
            "nom produit",
            "Active Compound",
            "Autres",
            "dose retenue",
            "Dose",
            "Biocontrôle (1/0)",
            "nombre max d'application",
        ]
    ]

    return df_soufre


def create_df_insecticide(df_bio_vigne_main_authorised_with_others_compounds):
    """
    Creates a DataFrame containing information about insecticide products.

    Args:
        df_bio_vigne_main_authorised_with_others_compounds (pd.DataFrame): Input DataFrame containing product information.

    Returns:
        pd.DataFrame: DataFrame containing insecticide product information.

    The function filters the input DataFrame based on the 'fonctions' column, selecting rows where the term
    'insecticide' is present. It then further filters based on specified active substances: 'Spinosad', 'Bacillus',
    and 'pyréthrines'. The 'Concentration' column is derived from the 'Substances actives' column, and concentration
    values are processed using the 'process_concentration' function. For products containing 'Bacillus', the concentration
    is set to 0 by convention.

    The 'Dose' column is calculated based on the concentration and 'dose retenue'. Additionally, a 'Biocontrôle (1/0)'
    column is created, with 1 indicating the presence of 'biocontrôle' in the 'mentions autorisees', and 0 otherwise.

    The resulting DataFrame includes columns: 'nom produit', 'Active Compound', 'Autres', 'dose retenue', 'Dose',
    'Biocontrôle (1/0)', and 'nombre max d'application'.
    """
    df_insecticide = df_bio_vigne_main_authorised_with_others_compounds[
        df_bio_vigne_main_authorised_with_others_compounds["fonctions"].str.contains(
            "insecticide", case=False, na=False
        )
    ].reset_index(drop=True)

    substances_to_include = ["Spinosad", "Bacillus", "pyréthrines"]

    df_insecticide = df_insecticide[
        df_insecticide["Substances actives"].apply(
            lambda x: any(sub in x for sub in substances_to_include)
        )
    ].reset_index(drop=True)

    df_insecticide["Concentration"] = (
        df_insecticide["Substances actives"].str.split(")").str[1]
    )
    df_insecticide["Concentration"] = df_insecticide["Concentration"].apply(
        process_concentration
    )

    # By convention, when the product is Bacillus the concentration is set to 0
    df_insecticide.loc[
        df_insecticide["Substances actives"].str.contains(
            "Bacillus", case=False, na=False
        ),
        "Concentration",
    ] = 0

    df_insecticide["Dose"] = (
        ((df_insecticide["Concentration"] / 100) * df_insecticide["dose retenue"])
        .astype(float)
        .round(3)
    )

    df_insecticide["Biocontrôle (1/0)"] = df_insecticide["mentions autorisees"].apply(
        lambda x: 1 if "biocontrôle" in x.lower() else 0
    )

    df_insecticide = df_insecticide[
        [
            "nom produit",
            "Active Compound",
            "Autres",
            "dose retenue",
            "Dose",
            "Biocontrôle (1/0)",
            "nombre max d'application",
        ]
    ]

    return df_insecticide


def create_df_pheromones(df_bio_vigne_main_authorised_with_others_compounds):
    """
    Creates a DataFrame containing information about pheromone products.

    Args:
        df_bio_vigne_main_authorised_with_others_compounds (pd.DataFrame): Input DataFrame containing product information.

    Returns:
        pd.DataFrame: DataFrame containing pheromone product information.

    * The function filters the input DataFrame based on the presence of 'pheromones' in the 'Substances actives' column.
    * The 'Active Compound' column is derived from the 'Substances actives' column, with some specific replacements.
    * The 'Biocontrôle (1/0)' column is created, with 1 indicating the presence of 'biocontrôle' in the 'mentions autorisees',
        and 0 otherwise.

    The resulting DataFrame includes columns: 'nom produit', 'Active Compound', and 'Biocontrôle (1/0)'.
    """
    df_pheromones = df_bio_vigne_main_authorised_with_others_compounds[
        df_bio_vigne_main_authorised_with_others_compounds[
            "Substances actives"
        ].str.contains("pheromones", case=False, na=False)
    ].reset_index(drop=True)

    df_pheromones["Active Compound"] = (
        df_pheromones["Substances actives"]
        .str.replace("(Straight Chain Lepidopteran Pheromones)", "")
        .replace("|", "+")
    )

    df_pheromones["Biocontrôle (1/0)"] = df_pheromones["mentions autorisees"].apply(
        lambda x: 1 if "biocontrôle" in x.lower() else 0
    )

    df_pheromones = df_pheromones[
        ["nom produit", "Active Compound", "Biocontrôle (1/0)"]
    ]
    return df_pheromones


def create_df_others(df_bio_vigne_main_authorised_with_others_compounds):
    """
    Creates a DataFrame for substances excluding specified substances and computes additional columns.

    Parameters:
        df_bio_vigne_main_authorised_with_others_compounds (pd.DataFrame): Input DataFrame containing product information.

    Returns:
        pd.DataFrame: DataFrame containing processed rows excluding specified substances.

    The function filters the input DataFrame based on the absence of specified substances in a case-insensitive manner.
    The resulting DataFrame includes all rows excluding the specified substances and computes additional columns:
    * 'Concentration': Extracts concentration information from the 'Substances actives' column.
    * 'Dose': Computes the dose based on concentration and 'dose retenue'.
    * 'Insecticide': Flags rows where 'fonctions' column contains the term 'insecticide'.
    * 'Biocontrôle (1/0)': Flags rows where 'mentions autorisees' column contains the term 'biocontrôle'.
    """
    substances_to_exclude = [
        "Spinosad",
        "Bacillus",
        "pyréthrines",
        "soufre",
        "Sulphur",
        "cuivre",
        "pheromones",
    ]
    df_others = df_bio_vigne_main_authorised_with_others_compounds[
        ~df_bio_vigne_main_authorised_with_others_compounds["Substances actives"].apply(
            lambda x: any(sub.lower() in x.lower() for sub in substances_to_exclude)
        )
    ].reset_index(drop=True)

    df_others["Concentration"] = df_others["Substances actives"].str.split(")").str[1]

    df_others["Concentration"] = df_others["Concentration"].apply(process_concentration)
    df_others["Dose"] = (
        ((df_others["Concentration"] / 100) * df_others["dose retenue"])
        .astype(float)
        .round(1)
    )

    df_others["Insecticide"] = df_others["fonctions"].apply(
        lambda x: 1 if "insecticide" in x.lower() else 0
    )

    df_others["Biocontrôle (1/0)"] = df_others["mentions autorisees"].apply(
        lambda x: 1 if "biocontrôle" in x.lower() else 0
    )
    df_others = df_others[
        [
            "nom produit",
            "Active Compound",
            "Autres",
            "dose retenue",
            "Dose",
            "Insecticide",
            "Biocontrôle (1/0)",
            "nombre max d'application",
        ]
    ]

    return df_others


def combine_products(df_bio_vigne_main_authorised_with_others_compounds):
    def rename_columns(df):
        """
        Combine different DataFrames for specific products into a single DataFrame.

        Parameters:
        - df_bio_vigne_main_authorised_with_others_compounds (pd.DataFrame): The main DataFrame containing information about authorized products.

        Returns:
        - pd.DataFrame: The combined DataFrame with renamed columns.
        """

        column_mapping = {
            "nom produit": "Spécialité commerciale",
            "Active Compound": "Matière active (M.A.)",
            "Autres": "Autre",
            "Concentration": "Concentration en M.A. (% )",
            "dose retenue": "Dose d'homologation (en kg ou L / ha)",
            "Dose": "Dose d'homologation (en kg ou L / ha)",
            "nombre max d'application": "Nombre de traitements autorisés",
            "Biocontrôle (1/0)": "New Biocontrol Column",  # Example with a column not present in the DataFrame
        }

        existing_columns = set(df.columns)
        mapping = {
            old_name: new_name
            for old_name, new_name in column_mapping.items()
            if old_name in existing_columns
        }

        return df.rename(columns=mapping)

    df_cuivre = rename_columns(
        create_df_cuivre(df_bio_vigne_main_authorised_with_others_compounds)
    )
    df_soufre = rename_columns(
        create_df_soufre(df_bio_vigne_main_authorised_with_others_compounds)
    )
    df_insecticide = rename_columns(
        create_df_insecticide(df_bio_vigne_main_authorised_with_others_compounds)
    )
    df_pheromones = rename_columns(
        create_df_pheromones(df_bio_vigne_main_authorised_with_others_compounds)
    )
    df_others = df_others = rename_columns(
        create_df_others(df_bio_vigne_main_authorised_with_others_compounds)
    )

    df_combined_products = pd.concat(
        [df_cuivre, df_soufre, df_insecticide, df_pheromones, df_others], axis=1
    )

    return df_combined_products


def get_amm(file_name="amm.csv"):
    """
    Download, clean, and process AMM (Autorisation de Mise sur le Marché) data related to vine products.

    Args:
        file_name (str, optional): The name of the CSV file to save the processed data. Default is "amm.csv".

    Returns:
        int: Return value 0 indicating successful execution.
    """
    amm_url = get_url()
    df_amm = download_and_extract_dataframe(amm_url)

    df_bio_vigne_main_authorised = clean_df_amm(df_amm)
    df_bio_vigne_main_authorised_with_others = add_others(df_bio_vigne_main_authorised)
    df_bio_vigne_main_authorised_with_others_compounds = get_active_compound(
        df_bio_vigne_main_authorised_with_others
    )

    df_combined_products = combine_products(
        df_bio_vigne_main_authorised_with_others_compounds
    )
    df_combined_products.to_csv(file_name, sep=";", index=False)

    return 0
