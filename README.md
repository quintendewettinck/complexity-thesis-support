# complexity-thesis-support

A repository with example code for master's thesis students performing complexity-relatedness analyses.

Please refer to the document `thesis_instructions.pdf` on Toledo for general instructions.

## Usage

1. Download the raw data from the data sources specified below
2. Specify the path to your raw data in `RAW_DATA_PATH` in the `data_processing.R` script
3. Specify the `baci_version`, `HS_version` and `HS_digits`, as well as the years for which you have BACI data in the `data_processing.R` script
4. Run the `data_processing.R` script, which will: 
    a. Import the BACI trade data
    b. Aggregate the trade flows to the specified number of digits (`HS_digits`)
    c. Add the country ISO3 and product HS codes and descriptions
    d. Filter the dataset on the selected country and product sample (removing small nodes)
    e. Compute RCA_cp, M_cp, diversity_c, ubiquity_p, ECI_c, PCI_p, proximity_pp, and relatedness_cp (dens_cp)
5. Create new script(s) for your analysis
    
Note that the `data_processing.R` script serves as supporting material for the preprocessing of the data, and does not include the complete set of scripts used for the full analysis. 

## Data Sources

- CEPII BACI international trade data (https://www.cepii.fr/CEPII/en/bdd_modele/bdd_modele_item.asp?id=37)
- UN HS product descriptions (https://unstats.un.org/unsd/classifications/Econ)
  - Download "All HS codes and descriptions" xlsx file for product descriptions for all HS digits

## Directory Structure

``` text
├── analysis
│   └── README.txt
├── complexity-thesis-support.Rproj
├── data
│   ├── data_processed
│   └── data_raw
├── LICENSE
├── processing
│   └── data_processing.R
└── README.md
```

## Citation

For more details on the calculation and interpretation of the metrics, see

De Wettinck, Q., De Bruyne, K., & Bam, W. (2025). *Economic Complexity: Promise and Potential for Sustainable Development Decision Support* (SSRN Scholarly Paper No. 5412234). Social Science Research Network. https://doi.org/10.2139/ssrn.5412234

`BibTeX`: 
```bibtex
@misc{DeWettinck.etal2025,
  type = {{{SSRN Scholarly Paper}}},
  title = {Economic {{Complexity}}: {{Promise}} and {{Potential}} for {{Sustainable Development Decision Support}}},
  shorttitle = {Economic {{Complexity}}},
  author = {De Wettinck, Quinten and De Bruyne, Karolien and Bam, Wouter},
  year = 2025,
  number = {5412234},
  publisher = {Social Science Research Network},
  address = {Rochester, NY},
  doi = {10.2139/ssrn.5412234},
  urldate = {2025-10-22},
  archiveprefix = {Social Science Research Network},
  langid = {english}
}
```