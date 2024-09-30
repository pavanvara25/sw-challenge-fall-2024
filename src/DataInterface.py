"""
File: DataInterface.py
Author: Pavan Varakantham
Date: 09/29/2024
Description: A data processing interface that accepts time intervals, aggregates the data, and generates flat flies containing
             Open-High-Low-Close-Volume (OHLCV) bars for the specified intervals.
"""
import re
import datetime
import threading

class DataInterface:
    """
    A class to handle the generation and saving of OHLCV data from a cleaned dataset.

    This class provides methods to validate and parse time intervals, generate OHLCV 
    (Open, High, Low, Close, Volume) data for specified time periods, and save the 
    resulting data to a CSV file.

    Attributes:
        data (list): The cleaned trading data records, each a list of [timestamp, price, size].
    """

    def __init__(self, data_cleaner, num_threads=8):
        """
        Initialize the interface with the cleaned dataset.
        
        :param data: List of records (each record is a list of [timestamp, price, size]).
        """
        self.data = data_cleaner.get_cleaned_data()
        self.ohlcv_data = []
        self.num_threads = num_threads
        self.lock = threading.Lock()

    def validate_time_interval(self, interval):
        """
        Validate the format of a time interval string.

        :param interval: A string representing the time interval to be validated (e.g., '4s', '15m', '2h', '1d', '1h30m').
    
        This method uses a regular expression to check if the provided time interval string 
        follows a valid format. It can match intervals specified in days (d), hours (h), 
        minutes (m), and seconds (s). The method supports combinations of these units, 
        such as "1h30m" for 1 hour and 30 minutes.

        If the interval format is invalid, a ValueError is raised with a descriptive error message.

        :return: A match object if the format is valid, which contains the parsed components 
             of the time interval.
        """
        # Regex pattern to match time intervals (e.g., "4s", "15m", "2h", "1d", "1h30m")
        pattern = r'^(?:(\d+)d)?(?:(\d+)h)?(?:(\d+)m)?(?:(\d+)s)?$'
        
        # Use the regex pattern to match the provided interval string
        match = re.match(pattern, interval)
        
        # If the match is None, it means the interval format is invalid
        if not match:
            raise ValueError(f"Invalid time interval format: {interval}")
        
        # Return the match object, which contains the parsed components
        return match

    def parse_time_interval(self, interval):
        """
        Parse a time interval string and convert it into a timedelta object.

        :param interval: A string representing the time interval (e.g., '1m' for 1 minute, '500ms' for 500 milliseconds).
    
        This method validates the provided time interval string and extracts the 
        number of days, hours, minutes, and seconds from it. It then creates and 
        returns a `datetime.timedelta` object representing the specified duration.

        The format for the interval can include combinations of days, hours, minutes, 
        and seconds, which will be parsed accordingly. The method uses regular expressions
        to ensure the input is valid and structured correctly.

        :return: A `datetime.timedelta` object representing the parsed time interval.
        """
        # Validate the input interval string format using the validate_time_interval method
        match = self.validate_time_interval(interval)
        
        # Extract days, hours, minutes, and seconds from the match object; default to 0 if not present
        days = int(match.group(1) or 0)
        hours = int(match.group(2) or 0)
        minutes = int(match.group(3) or 0)
        seconds = int(match.group(4) or 0)
        
        # Create and return a timedelta object based on the extracted values
        return datetime.timedelta(days=days, hours=hours, minutes=minutes, seconds=seconds)
    
    def generate_ohlcv(self, start_time, end_time, interval):
        """
        Generate OHLCV (Open, High, Low, Close, Volume) data for the specified time interval.

        :param start_time: The starting time (datetime) for the OHLCV generation.
        :param end_time: The ending time (datetime) for the OHLCV generation.
        :param interval: A string representing the time interval (e.g., '1m' for 1 minute, '500ms' for 500 milliseconds).
    
        This method generates OHLCV data by iterating through the trade data and grouping trades into
        the specified time intervals. For each interval, the following values are calculated:
    
        - Open: The price of the first trade in the interval.
        - High: The highest trade price within the interval.
        - Low: The lowest trade price within the interval.
        - Close: The price of the last trade in the interval.
        - Volume: The total size (volume) of all trades within the interval.

        The resulting OHLCV data is stored in `self.data` and returned as a list of lists, 
        where each list contains [Timestamp, Open, High, Low, Close, Volume] for the respective interval.

        :return: A list of OHLCV data, where each entry is a row represented as a list 
             containing: [Timestamp, Open, High, Low, Close, Volume].
        """
        interval_timedelta = self.parse_time_interval(interval)

        # Check if the time period is less than the interval
        if (end_time - start_time) < interval_timedelta:
            raise ValueError("The time period is shorter than the specified interval.")
        
        # Split data into chunks for processing
        chunk_size = len(self.data) // self.num_threads
        threads = []
        
        for i in range(self.num_threads):
            start_index = i * chunk_size
            end_index = (i + 1) * chunk_size if i != self.num_threads - 1 else len(self.data)
            thread_data = self.data[start_index:end_index]

            thread = threading.Thread(target=self._process_data_chunk, args=(thread_data, start_time, end_time, interval_timedelta))
            threads.append(thread)
            thread.start()

        # Wait for all threads to complete
        for thread in threads:
            thread.join()
    
    def _process_data_chunk(self, data_chunk, start_time, end_time, interval_timedelta):
        """
        Process a chunk of tick data to generate OHLCV (Open, High, Low, Close, Volume) bars for a specified time interval.
    
        Args:
            data_chunk (list): A list of rows where each row contains [timestamp, price, size].
            start_time (datetime): The start time of the period to process.
            end_time (datetime): The end time of the period to process.
            interval_timedelta (timedelta): The time interval for OHLCV aggregation.
    
        This method processes tick data within the provided time range, grouping data into OHLCV bars for each interval.
        It locks shared resources (e.g., `self.ohlcv_data`) when appending results to ensure thread safety.
        """
        current_time = start_time
        
        # Loop through time intervals until the current time exceeds the end time
        while current_time < end_time:
            # Initialize OHLCV variables for the current interval
            open_price, high_price, low_price, close_price, total_volume = None, float('-inf'), float('inf'), None, 0
            interval_end_time = current_time + interval_timedelta

            # Only process if the interval end time does not exceed the overall end time
            if interval_end_time > end_time:
                break  # Exit the loop if the next interval exceeds the end time

            # Iterate through each row (tick) in the data chunk
            for row in data_chunk:
                # Parse trade time from the first column
                trade_time = datetime.datetime.strptime(row[0], "%Y-%m-%d %H:%M:%S.%f")
                
                # Check if the trade falls within the current time interval
                if current_time <= trade_time < interval_end_time:
                    price = float(row[1])
                    size = float(row[2])
                    # Set open price if it's the first trade in this interval
                    if open_price is None:
                        open_price = price
                    
                    # Update high, low, and close prices for this interval
                    high_price = max(high_price, price)
                    low_price = min(low_price, price)
                    close_price = price
                    total_volume += size # Accumulate total volume

            # If any trades were processed in this interval, append OHLCV data
            if open_price is not None:
                # Use lock to ensure thread-safe access to shared OHLCV data
                with self.lock:
                    self.ohlcv_data.append([current_time, open_price, high_price, low_price, close_price, total_volume])

            # Move to the next time interval
            current_time += interval_timedelta
    
    def write_to_csv(self, output_file):
        """
        Save OHLCV (Open, High, Low, Close, Volume) data to a CSV file.

        :param output_file: The path where the CSV file will be saved.
        :param ohlcv_data: A list of OHLCV data, where each entry is a row represented as a list 
                       containing: [Timestamp, Open, High, Low, Close, Volume].
    
        This method writes the OHLCV data to a CSV file. The first line of the CSV file will contain
        the column headers: 'Timestamp', 'Open', 'High', 'Low', 'Close', and 'Volume'. Each subsequent
        line will contain the OHLCV data from the input list.
        """
        with open(output_file, 'w') as file:
            file.write("Timestamp, Open, High, Low, Close, Volume\n") # Write header
            for row in self.ohlcv_data:
                # Write each row in the OHLCV data
                file.write(f"{row[0]},{row[1]},{row[2]},{row[3]},{row[4]},{row[5]}\n")
        print(f"OHLCV data successfully saved to {output_file}")

    def display_data(self):
        """
        Dispay the OHLCV data
        """

        print("Timestamp, Open, High, Low, Close, Volume\n") # Write header
        if self.ohlcv_data:
            for row in self.ohlcv_data:
                print(row)
            else:
                print("No data loaded")