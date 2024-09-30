"""
File: Main.py
Author: Pavan Varakantham
Date: 09/29/2024
Description: Main script for loading, cleaning, and generating OHLCV data from tick data.
"""
from DataLoader import DataLoader
from DataCleaner import DataCleaner
from DataInterface import DataInterface
import datetime
import os

def main():
    """
    Main function to execute the data loading, cleaning, and OHLCV generation process.

    This function initializes the DataLoader to load tick data from CSV files,
    cleans the loaded data using the DataCleaner, and prompts the user for input 
    to generate OHLCV data for a specified time interval. The generated OHLCV data 
    is then saved to a CSV file.
    """
    # Initialize DataLoader and load data
    data_loader = DataLoader(directory='data')
    data_loader.load_data()  # Ensure the data is loaded
    data_loader.write_to_csv('combined_data.csv')

    # Clean the data
    data_cleaner = DataCleaner(data_loader)
    data_cleaner.clean_data()  # Perform cleaning

    data_cleaner.write_to_csv('cleaned_data.csv')

    ohlcv_generator = DataInterface(data_cleaner)
    
    # Prompt for start time
    start_time_str = input("Enter the start time (YYYY-MM-DD HH:MM:SS): ")
    start_time = datetime.datetime.strptime(start_time_str, '%Y-%m-%d %H:%M:%S')

    # Prompt for end time
    end_time_str = input("Enter the end time (YYYY-MM-DD HH:MM:SS): ")
    end_time = datetime.datetime.strptime(end_time_str, '%Y-%m-%d %H:%M:%S')

    # Prompt for time interval
    time_interval = input("Enter the time interval (e.g., '4s', '15m', '2h', '1d', '1h30m'): ")

    ohlcv_generator.generate_ohlcv(start_time, end_time, time_interval)

    ohlcv_generator.write_to_csv('ohlcv_data.csv')   

if __name__ == "__main__":
    main()