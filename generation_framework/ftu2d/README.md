# Human Reference Atlas 2D Functional Tissue Unit to OWLNETS converter

Uses the 2D FTU Digital Object in the [Human Reference Atlas](https://apps.humanatlas.io/kg-explorer/?do=ctann) (HRA) Knowledge Graph site
to generate a set of text files that comply with the OWLNETS format, as described [here](https://github.com/callahantiff/PheKnowLator/blob/master/notebooks/OWLNETS_Example_Application.ipynb).


# Content
- **ftu2d.py** - Does the following:
   - Reads the configuration file **ftu2d.ini**
   - Downloads the 2D FTU crosswalk CSV that corresponds to the SAB argument.
   - Generates files in OWLNETS format based on the spreadsheet.
- **ftu2d.ini** - INI file with URL links to the FTU2D files.


# Arguments
1. The SAB for the ontology--i.e., FTU2D

# Dependencies
1. Files in the **ubkg_utilities** folder:
   - ubkg_extract.py
   - ubkg_logging.py
   - ubkg_config.py
   - ubkg_parsetools.py
2. An application configuration file named **ftu2d.ini.**
3. The HRA FTU2D CSV.

# To run
1. Modify **ftu2d.ini**.
2. Configure the **sabs.json** file at the generation_framework root to call ftu2d.py with the appropriate SAB.


# Format of HRA FTU2D crosswalk CSV

The FTU2D CSV resolves to the level of "FTU part". Each row in the spreadsheet 
corresponds to a unique combination of organ, FTU, and FTU part.

# ETL Algorithm

## Nodes file
1. Define a **root node** for FTU2D.
2. Define a **parent node** for the _organ parent_.
3. Define a **parent node** for the _ftu_parent_.
4. Define a **parent node** for the _ftu_part_parent_.
5. Define unique nodes for the organs, FTUs, and FTU parts.
6. Define dbxrefs for nodes using the corresponding columns from the row.
   
## Edges file
Assert _isa_, _has_ftu_, and _has_ftu_part_ relationships.
