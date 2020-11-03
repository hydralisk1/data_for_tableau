# data_for_tableau

* csv_files_download.ipynb: Automatically download zip files from the Citi Bike webpage and extract all of them.
* getting_ny_station_info.ipynb: Get all Citi Bike station information from Citi Bike API server.
* creating_csv.ipynb: Load data in 2019 from the CSV downloaded, and create csv files that have bikeid, age, and station information.
* cleaning_csv.py: Read all the csv files downloaded, and create a csv file to use in tableau. It will take several hours to complete the work. The multiprocessing module has been used in order to improve performance.