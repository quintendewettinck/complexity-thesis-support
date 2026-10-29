# Complexity Thesis Support

This repository contains example code for master's thesis students performing complexity-relatedness analyses.

Please refer to the document `thesis_instructions.pdf` on Toledo for general instructions.

## Usage

1. Download the raw data from the data sources specified below
2. Specify the path to your raw data in `RAW_DATA_PATH` in the `data_processing.R` script
3. Specify the `baci_version`, `HS_version` and `HS_digits`, as well as the years for which you have BACI data in the `data_processing.R` script
4. Run the `data_processing.R` script, which will: 
    1. Import the BACI trade data
    2. Aggregate the trade flows to the specified number of digits (`HS_digits`); recommended number of digits is 4
    3. Add the country ISO3 and product HS codes and descriptions
    4. Filter the dataset on the selected country and product sample (removing small nodes)
    5. Compute RCA_cp, M_cp, diversity_c, ubiquity_p, ECI_c, PCI_p, proximity_pp, and relatedness_cp (dens_cp)
    6. Export the country-product-year level dataset to `cp_df.csv`
5. Create new script(s) for your analysis
    
Note that the `data_processing.R` script serves as supporting material for the preprocessing of the data, and does not include *all* relevant code required for the full analysis. 

## Project Structure

This repository is structured as follows:

``` bash
.
├── README.md              # this file
├── data
│   ├── raw                # raw data
│   └── processed          # processed/clean data
├── processing
│   └── data_processing.R  # script that preprocesses all data for analysis
└── analysis               # analysis scripts
```

## Data Sources

- CEPII BACI international trade data (HS07, 2007-2023) (https://www.cepii.fr/CEPII/en/bdd_modele/bdd_modele_item.asp?id=37)
- UN HS product descriptions (https://unstats.un.org/unsd/classifications/Econ)
  - Download "All HS codes and descriptions" xlsx file for product descriptions for all HS digits
- JLOP Industrial Policy Data (https://github.com/industrialpolicygroup/IndustrialPolicyData/blob/main/data/JLOP_2025.xlsx)
  - Use this data to *support* your industrial policy analysis (this does not replace your analysis of government reports and documents)

## Citation

Please cite this repository as 

De Wettinck, Q. (2025). Complexity Thesis Support [Source code]. https://github.com/quintendewettinck/complexity-thesis-support

`BibTeX`:
```bibtex
@software{De_Wettinck_Complexity_Thesis_Support_2025,
  author = {De Wettinck, Quinten},
  license = {MIT},
  month = oct,
  title = {{Complexity Thesis Support}},
  url = {https://github.com/quintendewettinck/complexity-thesis-support},
  year = {2025}
}
```

For more details on the calculation and interpretation of the metrics, see

De Wettinck, Q., De Bruyne, K., & Bam, W. (2025). *Economic Complexity: Promise and Potential for Sustainable Development Decision Support* (SSRN Scholarly Paper No. 5412234). Social Science Research Network. https://doi.org/10.2139/ssrn.5412234

`BibTeX`:
```bibtex
@misc{DeWettinck.etal2025a,
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

and 

De Wettinck, Q., De Bruyne, K., Bam, W., & Hidalgo, C. A. (2025). *Economic Complexity Alignment and Sustainable Development* (arXiv:2509.17919). arXiv. https://doi.org/10.48550/arXiv.2509.17919

`BibTeX`:
```bibtex
@misc{DeWettinck.etal2025b,
  title = {Economic {{Complexity Alignment}} and {{Sustainable Development}}},
  author = {De Wettinck, Quinten and De Bruyne, Karolien and Bam, Wouter and Hidalgo, C{\'e}sar A.},
  year = 2025,
  month = sep,
  number = {arXiv:2509.17919},
  eprint = {2509.17919},
  primaryclass = {econ},
  publisher = {arXiv},
  doi = {10.48550/arXiv.2509.17919},
  urldate = {2025-09-23},
  archiveprefix = {arXiv},
}
