# CTG SWE Task: Tick Data Processing Project
This project is designed to efficiently process large datasets related to stock trading. It includes three main components: **DataLoader**, **DataCleaner**, and **DataInterface**. These components work together to load, clean, and aggregate tick data into OHLCV bars for specified time intervals.

## Table of Contents
- [Project Overview](#project-overview)
- [Components](#components)
  - [DataLoader](#dataloader)
  - [DataCleaner](#datacleaner)
  - [DataInterface](#datainterface)

## Project Overview
The primary goal of this project is to handle large volumes of stock tick data efficiently. The project features the following capabilities:
- Loading and combining thousands of CSV files containing tick data.
- Cleaning the data to identify and correct common issues.
- Aggregating tick data into OHLCV (Open, High, Low, Close, Volume) bars for specified time intervals.

## Components

### DataLoader
The **DataLoader** class is responsible for:
- Loading multiple CSV files containing tick data.
- Combining the data into a single dataset.
- Calculating statistical measures such as mean and standard deviation of the stock price values which are used in the DataCleaner class to remove misformatted data.
#### Example Usage
```python
# Initialize DataLoader and load data
data_loader = DataLoader(directory='data')
data_loader.load_data()

# Get the mean price and standard deviation
mean_price, std_dev_price = data_loader.get_mean_and_std_dev()

data_loader.write_to_csv('combined_data.csv')
```
- ```data_loader = DataLoader(directory='data')```: Initializes the DataLoader object and specifies the path where the CSV files are located.
- ```data_loader.load_data()```: Reads and combines all the CSV files into a single dataset.
- ```mean_price, std_dev_price = data_loader.get_mean_and_std_dev()```: Calculates the mean and standard deviation of the price values in the combined data.
- ```data_loader.write_to_csv('combined_data.csv')```: Saves the combined data to a new CSV file named combined_data.csv


### DataCleaner
The **DataCleaner** class is designed to:
- Clean the tick data by identifying and correcting common issues including negative prices, missing values, misformatted values, and timestamps outside of market hours (9:30 a.m. - 4:00 p.m.).
- Provide an interface for data verification before and after cleaning.
#### Example Usage
```python
# Clean the data
data_cleaner = DataCleaner('data_loader')
cleaned_data = cleaner.clean_data()

#Save the cleaned data
cleaner.print_cleaned_data()
```
- ```data_cleaner = DataCleaner('data_loader')```: Initializes the DataCleaner object with the combined data from the CSV file.
- ```data_cleaner.clean_data()```: Cleans the data by identifying and fixing common issues, such as misformatted values.
- ```data_cleaner.write_to_csv('cleaned_data.csv')```: Saves the cleaned data to a new CSV file named combined_data.csv.


### DataInterface
The **DataInterface** class is used for:
- Validating user inputs for interval formats and datetime values.
- Aggregating tick data into OHLCV (Open, High, Low, Close, Volume) bars for specified time intervals.
- Saving the aggregated data to a new CSV file.
#### Example Usage
```python
# Initialize DataInterface (ohlcv_generator) with the cleaned data
ohlcv_generator = DataInterface(data_cleaner)

# Prompt for start time
start_time_str = input("Enter the start time (YYYY-MM-DD HH:MM:SS): ")
start_time = datetime.datetime.strptime(start_time_str, '%Y-%m-%d %H:%M:%S')

# Prompt for end time
end_time_str = input("Enter the end time (YYYY-MM-DD HH:MM:SS): ")
end_time = datetime.datetime.strptime(end_time_str, '%Y-%m-%d %H:%M:%S')

# Prompt for time interval
time_interval = input("Enter the time interval (e.g., '4s', '15m', '2h', '1d', '1h30m'): ")

# Generate OHLCV bars for the specified time range and interval
ohlcv_generator.generate_ohlcv(start_time, end_time, time_interval)

# Save the OHLCV data to a CSV file
ohlcv_generator.write_to_csv('ohlcv_data.csv')
```
- ```DataInterface(data_cleaner)```: Initializes the DataInterface object (ohlcv_generator) with the cleaned data from the DataCleaner.
- *Prompts*: The code prompts the user for the start time, end time, and time interval for generating OHLCV bars.
- ```generate_ohlcv()```: This method generates OHLCV bars based on the user-provided time range and interval.
- ```write_to_csv('ohlcv_data.csv')```: Saves the generated OHLCV data to a CSV file named ohlcv_data.csv.
  
