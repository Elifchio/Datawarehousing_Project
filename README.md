Project created by 
Elif Yilmaz
Alice Pietriccioli

Dataset: https://data.cityofchicago.org/Transportation/Traffic-Crashes-Crashes/85ca-t3if/about_data

Project Overview: Data Warehouse & OLAP Cube Development
This project involved transforming raw data from a publicly available dataset into structured, relational data for analysis. 
The project followed a systematic approach that covered the entire data lifecycle, from initial understanding to the creation of an OLAP cube for business analytics.

Project Steps:

1)Data Understanding & Preparation (Data_Understanding.ipynb, data_preparation.py and recovering_loc_G14):
Explore and understand the data from three large CSV files. The data was then cleaned, 
formatted, and preprocessed to ensure it was ready for the next stage.
recovering_loc_G14 file includes the code snippet where some missing location information was recovered using GeoPy library.

2)Relational Data Warehouse Creation (table_creation.py) :
Structuring the data into tables that could be queried and joined. 

3)Uploading Data to SQL Server (table_uploading.py) :
Uploading tables to the server using SQL Server Management Studio (SSMS)

4)OLAP Cube Development:
The next step was to design and create an OLAP (Online Analytical Processing) cube using SQL Server Analysis Services (SSAS).
This cube was built to support multidimensional analysis, allowing for fast querying of the data along different dimensions, such as time, geography, and product categories.
Note: The OLAP cube itself is not available for direct viewing or interaction, as it was created in a private environment and hosted on a remote server 
for internal analysis. Therefore, the cube cannot be accessed externally. However, the MDX queries used to interact with the cube are available in this repository.


5)MDX Query Development (MDX_queries.mdx):
MDX (Multidimensional Expressions) queries to answer key business questions. Queries extracted insights from the OLAP cube, enabling data-driven decisions.

