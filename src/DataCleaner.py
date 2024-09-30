"""
File: DataCleaner.py
Author: Pavan Varakantham
Date: 09/**/2024
Description: A data cleaning pipeline to address four main issues: missing values, negative prices, 
"""

import csv
import datetime
import threading

class DataCleaner:
    """
    A class to clean and process trading data.

    This class is responsible for cleaning the dataset by removing invalid entries, correcting 
    negative prices, handling misformatted values, eliminating duplicates, and ensuring
    that the trading data falls within valid trading hours. The cleaned data can then be saved 
    to a CSV file for further analysis.

    Attributes:
        data (list): The raw trading data from DataLoader to be cleaned.
    """

    def __init__(self, data_loader, num_threads=8):
        self.data = data_loader.get_combined_data()
        self.mean_price, self.std_dev_price = data_loader.get_mean_and_std_dev()
        self.num_threads = num_threads
        self.cleaned_data = []
        self.data_lock = threading.Lock()
    
    def clean_data(self):
        """
        Clean the dataset using multiple threads.
        """
        threads = []
        data_chunks = self._split_data_among_threads()

        # Create and start threads
        for chunk in data_chunks:
            thread = threading.Thread(target=self._clean_data_chunk, args=(chunk,))
            threads.append(thread)
            thread.start()

        # Wait for all threads to finish
        for thread in threads:
            thread.join()

    def _split_data_among_threads(self):
        """
        Helper function to split the data into chunks for each thread.
        :return: A list of data chunks, one for each thread.
        """
        chunk_size = len(self.data) // self.num_threads
        return [self.data[i:i + chunk_size] for i in range(0, len(self.data), chunk_size)]
    
    def _clean_data_chunk(self, data_chunk):
        """
        Helper function to clean a chunk of data. This will be executed by each thread.
        
        :param data_chunk: A list of rows to be cleaned by the thread.
        """
        lower_threshold = self.mean_price - 2 * self.std_dev_price
        upper_threshold = self.mean_price + 2 * self.std_dev_price
        local_cleaned_data = []
        seen_rows = set()

        for row in data_chunk:
            timestamp, price, size = row

            if not self.is_valid_row(row, lower_threshold, upper_threshold):
                continue  # Skip invalid rows

            try:
                price = float(price)
                size = int(size)
            except ValueError:
                continue  # Skip misformatted rows

            if tuple(row) not in seen_rows:
                seen_rows.add(tuple(row))  # Track duplicates
                local_cleaned_data.append(row)  # Append valid row to local cleaned data

        # Safely append local cleaned data to shared cleaned data
        with self.data_lock:
            self.cleaned_data.extend(local_cleaned_data)

    def is_valid_row(self, row, lower_threshold, upper_threshold):
        """
        Check if a row is valid based on missing values, negative prices, and misformatted values.

        :param row: The row to be checked.
        :return: True if valid, False otherwise.
        """
        timestamp, price, size = row
        
        # Check for missing values
        if timestamp is None or price is None or size is None:
            return False  # Invalid row if any value is missing

        # Convert price for further checks
        try:
            price = float(price)
        except ValueError:
            return False  # Invalid if price cannot be converted

        # Check for negative price values
        if price < 0:
            return False  # Invalid if price is negative

        # Check for misformatted price values (trading range in the 400s)
        if price < lower_threshold or price > upper_threshold:
            return False  # Invalid if price is outside the acceptable range

        # Check if the trade occurred within valid trading hours (9:30 AM - 4:00 PM)
        try:
            trade_time = datetime.datetime.strptime(timestamp, '%Y-%m-%d %H:%M:%S.%f').time()
            start_time = datetime.time(9,30) # 9:30 AM
            end_time = datetime.time(16, 0) # 4:00 PM

            if not (start_time <= trade_time <= end_time):
                return False # Invalid if the trade is outside trading hours
        except ValueError:
            return False # Invalid if timestamp is in the wrong format
        return True  # Row is valid
    
    def write_to_csv(self, output_file):
        """
        Save the cleaned data to a CSV file.

        :param output_file: The path of the output CSV file.
        """
        with open(output_file, mode='w', newline='') as file:
            writer = csv.writer(file)
            # Optionally write a header if needed
            writer.writerow(['Timestamp', 'Price', 'Size'])  # Adjust according to your actual headers
            writer.writerows(self.cleaned_data)  # Write all the cleaned rows to the file
        print(f"Cleaned data successfully saved to {output_file}.")

    def save_data(self, output_file):
        """
        Save cleaned data to a CSV file.

        :param output_file: The path of the output CSV file.
        """
        # Create a DataCleaner instance to save cleaned data
        cleaner = DataCleaner(self.data)
        cleaner.save_cleaned_data_to_csv(output_file)

    def get_cleaned_data(self):
         """
        Concatenate all the DataFrames into a single DataFrame
        
        :return: a concatenated dataframe from the self.dataframes list if it exists, or an empty dataframe if the list is 
                empty or no data was loaded.
        """
         return self.cleaned_data