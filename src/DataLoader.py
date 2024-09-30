"""
File: DataLoader.py
Author: Pavan Varakantham
Date: 09/29/2024
Description: Loads and combines thousands of CSV files containing tick data for "CTG"
"""
import os
import csv
import threading

class DataLoader:
    """
    A class to load and manage trading data from CSV files.

    This class is responsible for loading CSV files from a specified directory into the program's memory. 
    It provides methods to:
    - Load data from CSV files and store it for processing.
    - Retrieve the combined data from all loaded CSV files.
    - Write the loaded data into a new CSV file.
    - Handle errors related to file operations and data parsing gracefully.

    Attributes:
        directory (str): The directory where CSV files are located.
        csv_files (list): A list of CSV files found in the specified directory.
        data (list): A list to store the loaded data from the CSV files.
    """

    def __init__(self, directory='data', num_threads=8):
        """
        Initialize the object with the specified directory, load CSV files, and set up data storage.
    
        :param directory: The directory where CSV files are located (default is 'data').
        """
        self.directory = directory
        self.csv_files = self.load_csv_files()
        self.data = []
        self.mean_price = None
        self.std_dev_price = None
        self.num_threads = num_threads
        self.data_lock = threading.Lock()
    
    def load_csv_files(self):
        """
        Load data from CSV files into program's heap memory
        
        :return: a list of .csv files in the 'data' directory, effectively filtering out non-CSV files
        """
        return [file for file in os.listdir(self.directory) if file.endswith('.csv')]

    def load_data(self):
        """
        Loop through the list of CSV files and read them to load them into the program's heap memory
        """
        price_values = []
        threads = []
        file_chunks = self._split_files_among_threads()

        # Create and start threads
        for chunk in file_chunks:
            thread = threading.Thread(target=self._load_files_chunk, args=(chunk, price_values))
            threads.append(thread)
            thread.start()

        # Wait for all threads to finish
        for thread in threads:
            thread.join()

        # Calculate mean and standard deviation after loading data
        if price_values:
            self.mean_price = sum(price_values) / len(price_values)
            variance = sum((x - self.mean_price) ** 2 for x in price_values) / len(price_values)
            self.std_dev_price = variance ** 0.5

    def _split_files_among_threads(self):
        """
        Helper function to split the list of CSV files into chunks for each thread.
        :return: A list of file chunks, one for each thread.
        """
        # Divide files as evenly as possible among threads
        chunk_size = len(self.csv_files) // self.num_threads
        return [self.csv_files[i:i + chunk_size] for i in range(0, len(self.csv_files), chunk_size)]

    def _load_files_chunk(self, csv_files_chunk, price_values):
        """
        Helper function to load a chunk of CSV files. This will be executed by each thread.
        :param csv_files_chunk: A list of CSV files to be processed by the thread
        :param price_values: The list of price values (shared among threads)
        """
        for csv_file in csv_files_chunk:
            file_path = os.path.join(self.directory, csv_file)
            try:
                with open(file_path, mode='r') as file:
                    reader = csv.reader(file)
                    header = next(reader)  # Skip the header
                    
                    # Read all rows from the CSV file
                    for row in reader:
                        with self.data_lock:  # Ensure thread-safe access to shared resources
                            self.data.append(row)
                        try:
                            price = float(row[1])  # Assuming the price is in the second column
                            if price is not None and price > 0:
                                price_values.append(price)
                        except (ValueError, IndexError):
                            continue  # Skip rows with invalid prices
            except FileNotFoundError:
                print(f"Error: {file_path} not found.")
            except csv.Error as e:
                print(f"Error: Could not parse {file_path} - {e}.")
            except Exception as e:
                print(f"Error: {e} while processing {file_path}")
    
    def get_combined_data(self):
        """
        Concatenate all the DataFrames into a single DataFrame
        
        :return: a concatenated dataframe from the self.dataframes list if it exists, or an empty dataframe if the list is 
                empty or no data was loaded.
        """
        return self.data

    def write_to_csv(self, output_file):
        """
        Write the combined data into a new CSV file
        
        :param output_file: the path of the output CSV file
        """
        
        # Open the specified output file in write mode, ensure newline characters are handled correctly
        with open(output_file, mode='w', newline='') as file:
            # Create a CSV writer object to handle writing rows to the file
            writer = csv.writer(file)

            # Check if the self.data attribute contains any data
            if self.data:
                writer.writerows(self.data)  # Write all the rows to the file
                print(f"Data successfully written to {output_file}")
            else:
                print("No data to write.") # If there is no data to write, notify the user
            
    def display_data(self):
        """ 
        Display the combined data 
        
        """
        all_data = self.get_combined_data()
        if all_data:
            for row in all_data:
                print(row)
        else:
            print("No data loaded.")
    
    def get_mean_and_std_dev(self):
        """Return the calculated mean and standard deviation."""
        return self.mean_price, self.std_dev_price