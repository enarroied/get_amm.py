import os
import zipfile
from io import BytesIO
from pathlib import Path
from zipfile import ZipFile

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


# Get the files from the source
file_name = "98f7cac6-6b29-4859-8739-51b825196959"
url = "https://www.data.gouv.fr/fr/datasets/r/" + file_name
wget.download(url)
# Extract the file that we need from the zip file
with zipfile.ZipFile("./" + file_name, "r") as zip_ref:
    listOfFileNames = zip_ref.namelist()
    for fileName in listOfFileNames:
        if fileName == "usages_des_produits_autorises_v3_utf8.csv":
            zip_ref.extract(fileName, "./")

# Create a new file with only products allowed for organic growing AND for vine growing
# Also remove usages that are not required for the final file
with open("usages_des_produits_autorises_v3_utf8.csv", "r+") as all_products, open(
    "fichier_bio", "w+"
) as bio:
    for i in all_products:
        if (
            "Utilisable en agriculture biologique" in i
            and "Vigne" in i
            and ("Retrait" not in i)
            and "Thrips" not in i
            and "Black rot" not in i
            and "Bactérioses" not in i
            and "Excoriose" not in i
            and "Erinose" not in i
            and "Cochenilles" not in i
            and "Aleurodes" not in i
            and "Pourriture grise" not in i
            and "Mouches" not in i
            and "Stad. Hivern. Ravageurs" not in i
            and "lack dead arm" not in i
            and "Esca" not in i
            and "Chenilles phytophages" not in i
            and "Eutypiose" not in i
            and "Acariens" not in i
        ):
            bio.write("{0}".format(i))
bio.close()
all_products.close()

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
        Autres = ""
        liste_lecture = i.split(";")
        # Verify that the product is not doubled in the list
        if liste_lecture[2] in already:
            continue
        else:
            already.append(liste_lecture[2])
        # Extract extra elements
        if "badigeonnage" in i:
            Autres = "badigeon"
        else:
            None
        if "jardin" in i:
            Autres = Autres + "Jardin autorisé"
        else:
            None

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
