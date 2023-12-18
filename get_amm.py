import os
import re
from io import BytesIO
from pathlib import Path
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
    ending with '-utf8.zip' from the specified root URL (government Open Data)

    Returns:
    str or int: If a matching URL is found, returns the URL as a string.
               If no matching URL is found, raise URLNotFoundError.
    """

    root_url = "https://www.data.gouv.fr/fr/datasets/donnees-ouvertes-du-catalogue-e-phy-des-produits-phytopharmaceutiques-matieres-fertilisantes-et-supports-de-culture-adjuvants-produits-mixtes-et-melanges/"
    root_html = requests.get(root_url)

    soup_amm = BeautifulSoup(root_html.text)

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
    1. Select rows where the column "mentions autorisees" contains the term
       "Utilisable en agriculture biologique" (case-insensitive).
    2. Further filter the DataFrame to rows where the column "identifiant usage"
       contains the term "vigne" (case-insensitive).
    3. Exclude rows where the column "mentions autorisees" contains any of the terms
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
    - If 'badigeon' is present in 'condition emploi', 'Autres' will contain 'badigeon'.
    - If 'jardin' is present in 'gamme usage', 'Autres' will contain 'Jardin autorisé'.
    - If both conditions are met, 'Autres' will contain 'badigeon|Jardin autorisé', separated by a pipe (|) character.
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
    The active compound in in French ("matière active")

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
                print(second_name)

                second_row = row.copy()
                second_row["nom produit"] = second_name

                df_second_name = pd.concat(
                    [df_second_name, second_row.to_frame().T], ignore_index=True
                )
                print(second_row)

    df_bio_vigne_main_authorised_with_others_compounds = pd.concat(
        [df_bio_vigne_main_authorised_with_others_compounds, df_second_name],
        ignore_index=True,
    )

    return df_bio_vigne_main_authorised_with_others_compounds


# Create a new file with the final format and read fichier_bio
# to extract and clean the data in it before writing in the new file
with open("fichier_bio", "r+") as lecture, open("fichiers_intrants", "w+") as intrants:
    already = []
    intrants.write(
        "CUIVRE;;;;;;;;;SOUFRE;;;;;;;;;INSECTICIDE;;;;;;;;;CONFUSION;;;;;BIOCONTROLE;;;;;;;;\n"
    )
    intrants.write(
        "Spécialité Commerciale;Matière Active (M.A.);Autre;Concentration en M.A. (%);Dose homologuée (Kg ou L / ha);Kg / ha de M.A.;Nombre de traitements max;;;Spécialité Commerciale;Matière Active (M.A.);Autre;Concentration en M.A. (%);Dose homologuée (Kg ou L / ha);Kg / ha de M.A.;Biocontrôle (1/0);Nombre de traitements max;;Spécialité Commerciale;Matière Active (M.A.);Autre;Concentration en M.A. (%);Dose homologuée (Kg ou L / ha);Kg / ha de M.A.;Biocontrôle (1/0);Nombre de traitements max;;Spécialité commerciale;Matière Active (M.A);Biocontrôle(1/0);;;Spécialité Commerciale;Matière Active (M.A.);Autre;Concentration en M.A. (%);Dose homologuée (Kg ou L / ha);Kg / ha de M.A.;Biocontrôle (1/0);Insecticide;Nombre de traitements max;\n"
    )
    for i in lecture:
        # Extract 'MA' :
        MA = liste_lecture[8].split("(")

        # Extract copper information :
        if "cuivre" in liste_lecture[8]:
            try:
                # Concentration needs to be converted to %:
                Concentration_liste = MA[1].split(") ")
                Concentration_brute = Concentration_liste[1]
                Concentration_nombre = ""
                for i in Concentration_brute:
                    if i.isdigit() == True:
                        Concentration_nombre = Concentration_nombre + i
                if "%" in Concentration_brute:
                    Concentration = int(Concentration_nombre) / 10
                # !!! EMPTY space before g --> not to get kg (kilos) per L
                elif " g/" in Concentration_brute:
                    Concentration = int(Concentration_nombre) / 100
                else:
                    Concentration = int(Concentration_nombre) / 100
            except:
                Concentration = 0

            # Dose calculation
            Dose = (Concentration / 100) * float(liste_lecture[17])
            Dose = round(Dose, 1)

            # Write into the file
            intrants.write(
                "{0};{1};{2};{3};{4};{5};{6};;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;\n".format(
                    liste_lecture[2],
                    MA[0],
                    Autres,
                    Concentration,
                    liste_lecture[17],
                    Dose,
                    liste_lecture[21],
                )
            )
            liste_autres = liste_lecture[3].split(" | ")
            for i in liste_autres:
                if i == "":
                    continue
                intrants.write(
                    "{0};{1};{2};{3};{4};{5};{6};;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;\n".format(
                        i,
                        MA[0],
                        Autres,
                        Concentration,
                        liste_lecture[17],
                        Dose,
                        liste_lecture[21],
                    )
                )

        # Extract sulphur information :
        elif "soufre" in liste_lecture[8] or "Sulphur" in liste_lecture[8]:
            try:
                Concentration_liste = MA[1].split(") ")
                Concentration_brute = Concentration_liste[1]
                Concentration_nombre = ""
                for i in Concentration_brute:
                    if i.isdigit() == True:
                        Concentration_nombre = Concentration_nombre + i
                if "%" in Concentration_brute:
                    Concentration = int(Concentration_nombre) / 10
                # !!! EMPTY space before g --> not to get kg (kilos) per L
                elif " g/" in Concentration_brute:
                    Concentration = int(Concentration_nombre) / 100
                else:
                    Concentration = int(Concentration_nombre) / 100
            except:
                Concentration = 0

            # Dose calcuation
            try:
                Dose = (Concentration / 100) * float(liste_lecture[17])
                Dose = round(Dose, 1)
            except:
                Dose = 0
            # Get if the product is classified as "biocontrole" (yes/no)
            if "biocontrôle" in liste_lecture[7]:
                biocontrole = 1
            else:
                biocontrole = 0

            # Write into the file
            intrants.write(
                ";;;;;;;;;{0};{1};{2};{3};{4};{5};{6};{7};;;;;;;;;;;;;;;;;;;;;;;;\n".format(
                    liste_lecture[2],
                    MA[0],
                    Autres,
                    Concentration,
                    liste_lecture[17],
                    Dose,
                    biocontrole,
                    liste_lecture[21],
                )
            )
            liste_autres = liste_lecture[3].split(" | ")
            for i in liste_autres:
                if i == "":
                    continue
                intrants.write(
                    ";;;;;;;;;{0};{1};{2};{3};{4};{5};{6};{7};;;;;;;;;;;;;;;;;;;;;;;;\n".format(
                        i,
                        MA[0],
                        Autres,
                        Concentration,
                        liste_lecture[17],
                        Dose,
                        biocontrole,
                        liste_lecture[21],
                    )
                )

        # Extract insecticide information
        elif "Insecticide" in liste_lecture[9] and (
            "Spinosad" in liste_lecture[8]
            or "Bacillus" in liste_lecture[8]
            or "pyréthrines" in liste_lecture[8]
        ):
            try:
                Concentration_liste = MA[1].split(") ")
                Concentration_brute = Concentration_liste[1]
                Concentration_nombre = ""
                for i in Concentration_brute:
                    if i.isdigit() == True:
                        Concentration_nombre = Concentration_nombre + i
                if "Bacillus" in liste_lecture[8]:
                    Concentration = 0
                else:
                    # Extra 0 because ".0" adds a 0 when we get the data
                    Concentration = float(Concentration_nombre) / 10000
            except:
                Concentration = 0

            # Dose calculation
            try:
                Dose = Concentration * float(liste_lecture[17])
                Dose = round(Dose, 3)
            except:
                Dose = 0
            # Get if the product is classified as "biocontrole" (yes/no)
            if "biocontrôle" in liste_lecture[7]:
                biocontrole = 1
            else:
                biocontrole = 0

            # Write into the file
            intrants.write(
                ";;;;;;;;;;;;;;;;;;{0};{1};{2};{3};{4};{5};{6};{7};;;;;;;;;;;;;;;\n".format(
                    liste_lecture[2],
                    MA[0],
                    Autres,
                    Concentration,
                    liste_lecture[17],
                    Dose,
                    biocontrole,
                    liste_lecture[21],
                )
            )
            liste_autres = liste_lecture[3].split(" | ")
            for i in liste_autres:
                if i == "":
                    continue
                intrants.write(
                    ";;;;;;;;;;;;;;;;;;{0};{1};{2};{3};{4};{5};{6};{7};;;;;;;;;;;;;;;\n".format(
                        i,
                        MA[0],
                        Autres,
                        Concentration,
                        liste_lecture[17],
                        Dose,
                        biocontrole,
                        liste_lecture[21],
                    )
                )

        # Extract pheromones information
        elif "Pheromones" in liste_lecture[8]:
            MA = (
                liste_lecture[8]
                .replace("(Straight Chain Lepidopteran Pheromones)", "")
                .replace("|", "+")
            )
            # Get if the product is classified as "biocontrole" (yes/no)
            if "biocontrôle" in liste_lecture[7]:
                biocontrole = 1
            else:
                biocontrole = 0

            # Write into the file
            intrants.write(
                ";;;;;;;;;;;;;;;;;;;;;;;;;;;{0};{1};{2};;;;;;;;;;;\n".format(
                    liste_lecture[2], MA, biocontrole
                )
            )
            liste_autres = liste_lecture[3].split(" | ")
            for i in liste_autres:
                if i == "":
                    continue
                intrants.write(
                    ";;;;;;;;;;;;;;;;;;;;;;;;;;;{0};{1};{2};;;;;;;;;;;\n".format(
                        i, MA, biocontrole
                    )
                )

        else:
            try:
                Concentration_liste = MA[1].split(") ")
                Concentration_brute = Concentration_liste[1]
                Concentration_nombre = ""
                for i in Concentration_brute:
                    if i.isdigit() == True:
                        Concentration_nombre = Concentration_nombre + i
                if "%" in Concentration_brute:
                    Concentration = int(Concentration_nombre) / 10
                # !!! EMPTY space before g --> not to get kg (kilos) per L
                elif " g/" in Concentration_brute:
                    Concentration = int(Concentration_nombre) / 100
                else:
                    Concentration = int(Concentration_nombre) / 100
            except:
                Concentration = 0
            # Dose calcultation
            try:
                Dose = (Concentration / 100) * float(liste_lecture[17])
                Dose = round(Dose, 1)
            except:
                Dose = 0
            if "biocontrôle" in liste_lecture[7]:
                biocontrole = 1
            else:
                biocontrole = 0
            if "Insecticide" in liste_lecture[9]:
                insecticide = 1
            else:
                insecticide = 0

            # write into the file
            intrants.write(
                ";;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;{0};{1};{2};{3};{4};{5};{6};{7};{8}\n".format(
                    liste_lecture[2],
                    MA[0],
                    Autres,
                    Concentration,
                    liste_lecture[17],
                    Dose,
                    biocontrole,
                    insecticide,
                    liste_lecture[21],
                )
            )
            liste_autres = liste_lecture[3].split(" | ")
            for i in liste_autres:
                if i == "":
                    continue
                intrants.write(
                    ";;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;{0};{1};{2};{3};{4};{5};{6};{7};{8}\n".format(
                        i,
                        MA[0],
                        Autres,
                        Concentration,
                        liste_lecture[17],
                        Dose,
                        biocontrole,
                        insecticide,
                        liste_lecture[21],
                    )
                )
    intrants.close()
    lecture.close()

# We create empty lists to generate the final file, with no gaps
list_cuivre = []
list_soufre = []
list_insecticide = []
list_confusion = []
list_biocontrole = []

# Append the onformation to the lists
with open("fichiers_intrants", "r+") as intrants:
    for intrant in intrants:
        # skip the first 2 lines
        if intrant[:6] == "CUIVRE":
            continue
        if intrant[:9] == "Spécialit":
            continue

        if intrant[0] != ";":
            intrant_cuivre = intrant[0:-35]
            list_cuivre.append(intrant_cuivre + ";")
        elif intrant[:10] != ";;;;;;;;;;":
            # intrant_soufre = intrant.split(';;;;;;;;;')[1]
            intrant_soufre = intrant[8:-25]
            list_soufre.append(intrant_soufre + ";")
        elif intrant[:19] != ";;;;;;;;;;;;;;;;;;;":
            intrant_insecticide = intrant.split(";;;;;;;;;;;;;;;;;;")[1].split(";;;")
            list_insecticide.append(intrant_insecticide[0] + ";")
        elif intrant[:28] != ";;;;;;;;;;;;;;;;;;;;;;;;;;;;":
            intrant_confusion = intrant.split(";;;;;;;;;;;;;;;;;;;;;;;;;;;")[1].split(
                ";;;"
            )
            list_confusion.append(intrant_confusion[0] + ";")
        else:
            intrant_biocontrole = intrant.split(";;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;")[1]
            list_biocontrole.append(intrant_biocontrole[:-1])
intrants.close()

# Append semicolons at the end of the lists so all the lists have same lenght
for i in range(len(list_cuivre), 500):
    list_cuivre.append(";;;;;;;")

for i in range(len(list_soufre), 500):
    list_soufre.append(";;;;;;;;")

for i in range(len(list_insecticide), 500):
    list_insecticide.append(";;;;;;;;")

for i in range(len(list_confusion), 500):
    list_confusion.append(";;;")

for i in range(len(list_biocontrole), 500):
    list_biocontrole.append(";;;;;;;;")

# Create the new file with created lists
with open("intrants_final.csv", "w+") as intrants:
    for i in range(0, 500):
        intrants.write(
            f"{list_cuivre[i]};{list_soufre[i]};{list_insecticide[i]};{list_confusion[i]};{list_biocontrole[i]}\n"
        )
intrants.close()

# Remove downloaded files
base_path = Path(__file__).parent
file_path = (base_path / file_name).resolve()
os.remove(file_path)
file_path = (base_path / "usages_des_produits_autorises_v3_utf8.csv").resolve()
os.remove(file_path)
