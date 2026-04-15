# Unified Biomedical Knowledge Graph
## CEDAR-ENTITY ingestion script

### Purpose
The script in this folder generates files in UBKG edges/nodes format for an ontology with SAB **CEDAR_ENITY**.
The files map CEDAR template nodes to HuBMAP and SenNet provenance entities.

### Script Content
- **cedar_entity2jkgen.py**: Maps CEDAR templates to provenance entities.
- **cedar_entity2jkgen.ini**: Configuration file for script

### Script File Dependencies
1. Files in the **utilities** folder:
   - ubkg_logging.py
   - ubkg_config.py
2. An application configuration file named **cedar_entity2jkgen.ini.** 
3. The following SABs should have been ingested into the ontology CSVs prior to the execution of this script:
   - HUBMAP
   - SENNET
   - CEDAR 








