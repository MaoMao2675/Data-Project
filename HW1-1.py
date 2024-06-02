# Generate file
import csv
from faker import Faker
import random
import os
import json
import os
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import threading
import boto3
from botocore.exceptions import NoCredentialsError
from collections import OrderedDict
import hashlib

# Initialize Faker
fake = Faker()

# List of US states
states = [
    'Alabama', 'Alaska', 'Arizona', 'Arkansas', 'California', 'Colorado', 'Connecticut',
    'Delaware', 'Florida', 'Georgia', 'Hawaii', 'Idaho', 'Illinois', 'Indiana', 'Iowa',
    'Kansas', 'Kentucky', 'Louisiana', 'Maine', 'Maryland', 'Massachusetts', 'Michigan',
    'Minnesota', 'Mississippi', 'Missouri', 'Montana', 'Nebraska', 'Nevada', 'New Hampshire',
    'New Jersey', 'New Mexico', 'New York', 'North Carolina', 'North Dakota', 'Ohio',
    'Oklahoma', 'Oregon', 'Pennsylvania', 'Rhode Island', 'South Carolina', 'South Dakota',
    'Tennessee', 'Texas', 'Utah', 'Vermont', 'Virginia', 'Washington', 'West Virginia',
    'Wisconsin', 'Wyoming'
]

# List of occupations (sample occupations)
occupations = [
    'Software Engineer', 'Data Scientist', 'Teacher', 'Nurse', 'Doctor', 'Lawyer',
    'Accountant', 'Electrician', 'Plumber', 'Mechanic', 'Chef', 'Waiter', 'Police Officer',
    'Firefighter', 'Pilot', 'Artist', 'Musician', 'Writer', 'Photographer', 'Journalist'
]


# Function to generate a record
def generate_record():
    ssn = fake.ssn()
    state = random.choice(states)
    occupation = random.choice(occupations)
    return [ssn, state, occupation]


# Number of records to generate
num_records = 1_000_000

# File to write the records to
output_file = 'million_records.csv'

# Write the data to a CSV file
with open(output_file, mode='w', newline='') as file:
    writer = csv.writer(file)
    # Write the header
    writer.writerow(['SSN', 'State', 'Occupation'])
    # Write the records
    for _ in range(num_records):
        writer.writerow(generate_record())

print(f'{num_records} records written to {output_file}')


# Build Write Ahead Log

class WriteAheadLog:
    def __init__(self, wal_dir, max_wal_entries=100, max_wal_files=5):
        self.wal_dir = wal_dir
        self.max_wal_entries = max_wal_entries
        self.max_wal_files = max_wal_files
        self.current_wal_file = None
        self.wal_counter = 0
        self.current_file_index = 0
        self._ensure_wal_dir_exists()
        self._initialize_current_wal_file()

    def _ensure_wal_dir_exists(self):
        os.makedirs(self.wal_dir, exist_ok=True)
        print(f"Ensured WAL directory exists: {self.wal_dir}")

    def _initialize_current_wal_file(self):
        wal_files = sorted([f for f in os.listdir(self.wal_dir) if f.endswith('.wal')])
        self.current_file_index = len(wal_files)
        self.current_wal_file_path = os.path.join(self.wal_dir, f"{self.current_file_index}.wal")
        self.current_wal_file = open(self.current_wal_file_path, 'a')
        print(f"Initialized WAL file: {self.current_wal_file_path}")

    def _rotate_wal_file(self):
        self.current_wal_file.close()
        self.current_file_index += 1
        self.current_wal_file_path = os.path.join(self.wal_dir, f"{self.current_file_index}.wal")
        self.current_wal_file = open(self.current_wal_file_path, 'a')
        self.wal_counter = 0
        print(f"Rotated WAL file: {self.current_wal_file_path}")
        self._cleanup_old_wal_files()

    def _cleanup_old_wal_files(self):
        wal_files = sorted([f for f in os.listdir(self.wal_dir) if f.endswith('.wal')])
        while len(wal_files) > self.max_wal_files:
            oldest_file = wal_files.pop(0)
            os.remove(os.path.join(self.wal_dir, oldest_file))
            print(f"Deleted old WAL file: {oldest_file}")

    def log(self, entry):
        if self.wal_counter >= self.max_wal_entries:
            self._rotate_wal_file()
        self.current_wal_file.write(json.dumps(entry) + '\n')
        self.current_wal_file.flush()
        os.fsync(self.current_wal_file.fileno())
        self.wal_counter += 1
        print(f"Logged entry: {entry}")

    def start_transaction(self):
        self.log({'action': 'start'})

    def end_transaction(self):
        self.log({'action': 'end'})

    def log_insert(self, key, value):
        self.log({'action': 'insert', 'key': key, 'value': value, 'old_value': None})

    def log_update(self, key, old_value, new_value):
        self.log({'action': 'update', 'key': key, 'value': new_value, 'old_value': old_value})

    def log_delete(self, key, old_value):
        self.log({'action': 'delete', 'key': key, 'value': None, 'old_value': old_value})

    def load_wal(self):
        wal_files = sorted([f for f in os.listdir(self.wal_dir) if f.endswith('.wal')])
        transactions = []
        for wal_file in wal_files:
            with open(os.path.join(self.wal_dir, wal_file), 'r') as file:
                transaction = []
                for line in file:
                    log_entry = json.loads(line.strip())
                    if log_entry['action'] == 'start':
                        transaction = []
                    elif log_entry['action'] == 'end':
                        transactions.append(transaction)
                    else:
                        transaction.append(log_entry)
        print(f"Loaded WAL transactions: {transactions}")
        return transactions


# Build KVShard class

class KVShard:
    def __init__(self, shard_id, wal_dir, data_file, index_file, s3_bucket, s3_prefix,
                 aws_access_key_id, aws_secret_access_key):
        self.shard_id = shard_id
        self.data = {}
        self.index = {}
        self.wal = WriteAheadLog(wal_dir)
        self.data_file = data_file
        self.index_file = index_file
        self.s3_bucket = s3_bucket
        self.s3_prefix = s3_prefix
        self.s3_client = boto3.client(
            's3',
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key
        )
        self.lock = threading.Lock()
        self.load_wal()
        self.load_data()

    def load_wal(self):
        """Load and process WAL files to restore the state."""
        transactions = self.wal.load_wal()
        for transaction in transactions:
            for log_entry in transaction:
                self.apply_change(log_entry, apply_to_data=False)
        print(f"Loaded data from WAL for shard {self.shard_id}")

    def load_data(self):
        """Load data and index from Parquet files."""
        if os.path.exists(self.data_file):
            df = pd.read_parquet(self.data_file)
            self.data = df.set_index('key').to_dict()['value']
        if os.path.exists(self.index_file):
            df = pd.read_parquet(self.index_file)
            self.index = df.set_index('value').groupby(level=0).agg(list).to_dict()['key']
        print(f"Loaded data from Parquet for shard {self.shard_id}")

    def apply_change(self, log_entry, apply_to_data=True):
        """Apply a logged change to the data structure and secondary index."""
        key = log_entry['key']
        value = log_entry['value']
        old_value = log_entry['old_value']
        action = log_entry['action']
        if apply_to_data:
            if action == 'insert' or action == 'update':
                self.data[key] = value
                if old_value:
                    self.index[old_value].remove(key)
                    if not self.index[old_value]:
                        del self.index[old_value]
                if value not in self.index:
                    self.index[value] = []
                self.index[value].append(key)
            elif action == 'delete':
                del self.data[key]
                self.index[value].remove(key)
                if not self.index[value]:
                    del self.index[value]

    def save_to_parquet(self):
        """Save the data and index to Parquet files."""
        data_df = pd.DataFrame(list(self.data.items()), columns=['key', 'value'])
        data_df.to_parquet(self.data_file, index=False)
        index_df = pd.DataFrame([(k, v) for k, keys in self.index.items() for v in keys], columns=['value', 'key'])
        index_df.to_parquet(self.index_file, index=False)
        print(f"Saved data to Parquet for shard {self.shard_id}")

    def start_transaction(self):
        """Start a new transaction."""
        self.wal.start_transaction()

    def end_transaction(self):
        """End the current transaction."""
        self.wal.end_transaction()
        self.save_to_parquet()

    def insert(self, key, value):
        """Insert a new key-value pair."""
        with self.lock:
            old_value = self.data.get(key)
            self.wal.log_insert(key, value)
            self.apply_change({'action': 'insert', 'key': key, 'value': value, 'old_value': old_value})

    def update(self, key, value):
        """Update an existing key-value pair."""
        with self.lock:
            old_value = self.data.get(key)
            self.wal.log_update(key, old_value, value)
            self.apply_change({'action': 'update', 'key': key, 'value': value, 'old_value': old_value})

    def delete(self, key):
        """Delete a key-value pair."""
        with self.lock:
            value = self.data.get(key)
            self.wal.log_delete(key, value)
            self.apply_change({'action': 'delete', 'key': key, 'value': value, 'old_value': None})

    def get(self, key):
        """Retrieve a value by key."""
        with self.lock:
            return self.data.get(key)

    def backup_to_s3(self):
        """Upload WAL and Parquet files to S3."""
        try:
            # Upload WAL files
            wal_files = [f for f in os.listdir(self.wal.wal_dir) if f.endswith('.wal')]
            for wal_file in wal_files:
                self._upload_file_to_s3(os.path.join(self.wal.wal_dir, wal_file))

            # Upload Parquet files
            self._upload_file_to_s3(self.data_file)
            self._upload_file_to_s3(self.index_file)

            print(f"Backup completed for shard {self.shard_id}")
        except NoCredentialsError:
            print("Credentials not available for S3")

    def _upload_file_to_s3(self, file_path):
        """Helper method to upload a file to S3."""
        if os.path.exists(file_path):
            file_name = os.path.basename(file_path)
            s3_key = f"{self.s3_prefix}/{self.shard_id}/{file_name}"
            self.s3_client.upload_file(file_path, self.s3_bucket, s3_key)
            print(f"Uploaded {file_path} to s3://{self.s3_bucket}/{s3_key}")


# Build memory layer class

class MemoryLayer:
    def __init__(self, max_items=10000):
        self.max_items = max_items
        self.data = OrderedDict()

    def get(self, key):
        if key in self.data:
            # Move the accessed key to the end to show that it was recently used
            self.data.move_to_end(key)
            return self.data[key]
        return None

    def put(self, key, value):
        if key in self.data:
            # Move the existing key to the end
            self.data.move_to_end(key)
        self.data[key] = value
        if len(self.data) > self.max_items:
            # Remove the oldest item
            self.data.popitem(last=False)

    def __contains__(self, key):
        return key in self.data

    def save(self, file_path):
        df = pd.DataFrame(list(self.data.items()), columns=['Key', 'Value'])
        table = pa.Table.from_pandas(df)
        pq.write_table(table, file_path)
        print(f'Memory layer saved to {file_path}')

    def load(self, file_path):
        if os.path.exists(file_path):
            table = pq.read_table(file_path)
            df = table.to_pandas()
            self.data = OrderedDict(df.values.tolist())
            print(f'Memory layer loaded from {file_path}')


# distribute file records to shards and parquest files using hashing function

# Input file with 1 million records
input_file = 'million_records.csv'

# Output directory to store 1000 Parquet files
output_dir = 'hashed_parquet_files'
os.makedirs(output_dir, exist_ok=True)

# Number of output files
num_files = 1000

# Number of shards
num_shards = 4


# Function to hash SSN and determine the target file
def get_target_file(ssn, num_files):
    hash_value = int(hashlib.md5(ssn.encode()).hexdigest(), 16)
    return hash_value % num_files


# Initialize DataFrame dictionary for 1000 files
file_dataframes = {i: [] for i in range(num_files)}

# Read the input file and distribute records to corresponding dataframes
with open(input_file, mode='r') as file:
    reader = csv.reader(file)
    next(reader)  # Skip header row
    for row in reader:
        ssn, state, occupation = row
        target_file_index = get_target_file(ssn, num_files)
        file_dataframes[target_file_index].append(row)

# Write DataFrames to Parquet files
for i in range(num_files):
    file_name = os.path.join(output_dir, f'records_{i}.parquet')
    df = pd.DataFrame(file_dataframes[i], columns=['SSN', 'State', 'Occupation'])
    table = pa.Table.from_pandas(df)
    pq.write_table(table, file_name)

print(f'Records have been distributed to {num_files} Parquet files in {output_dir} directory')


# Function to hash and get shard index
def get_shard_index(ssn, num_shards):
    hash_value = int(hashlib.md5(ssn.encode()).hexdigest(), 16)
    return hash_value % num_shards


# Distribute records to 4 shards
shard_dirs = [f'shard_{i}' for i in range(num_shards)]
for shard_dir in shard_dirs:
    os.makedirs(shard_dir, exist_ok=True)

# Reading the previously distributed Parquet files and redistributing them into 4 shards
for i in range(num_files):
    file_name = os.path.join(output_dir, f'records_{i}.parquet')
    table = pq.read_table(file_name)
    df = table.to_pandas()
    for _, row in df.iterrows():
        ssn, state, occupation = row['SSN'], row['State'], row['Occupation']
        shard_index = get_shard_index(ssn, num_shards)
        shard_file = os.path.join(shard_dirs[shard_index], f'records_{i}.parquet')
        if not os.path.exists(shard_file):
            shard_df = pd.DataFrame(columns=['SSN', 'State', 'Occupation'])
        else:
            shard_df = pq.read_table(shard_file).to_pandas()
        shard_df = shard_df.append({'SSN': ssn, 'State': state, 'Occupation': occupation}, ignore_index=True)
        table = pa.Table.from_pandas(shard_df)
        pq.write_table(table, shard_file)

print(f'Records have been distributed to {num_shards} shards in {shard_dirs} directories')


# integration
def test_integration():
    wal_dir = 'test_wal_dir'
    data_file = 'test_data.parquet'
    index_file = 'test_index.parquet'
    s3_bucket = 'your-s3-bucket-name'
    s3_prefix = 'shard_backup'
    aws_access_key_id = 'AKI@A5RV6@6EHSZFQ67@74L'
    aws_secret_access_key = '0Wb4MaD8TW@@@kD0gqovMKevO@ZOYvYV1t@VDbAPx5roF'
    if not os.path.exists(wal_dir):
        os.makedirs(wal_dir)

    # Initialize KVShard
    shard = KVShard('shardA', wal_dir, data_file, index_file, s3_bucket, s3_prefix,
                    aws_access_key_id, aws_secret_access_key)

    # Initialize MemoryLayer
    memory_layer = MemoryLayer(max_items=10000)

    # Load sample data from CSV
    with open('million_records.csv', mode='r') as file1:
        reader1 = csv.reader(file1)
        next(reader1)  # Skip header row
        sample_data = [next(reader) for _ in range(5)]  # Read first 5 records for testing

    # Start a transaction and insert sample data
    shard.start_transaction()
    for ssn_test, state_test, occupation_test in sample_data:
        shard.insert(ssn_test, f'{state_test},{occupation_test}')
        memory_layer.put(ssn_test, f'{state_test},{occupation_test}')
    shard.end_transaction()

    # Retrieve values from memory layer
    for ssn_test, state_test, occupation_test in sample_data:
        print(f'{ssn_test} -> {memory_layer.get(ssn_test)}')  # Output: state,occupation

    # Update a key
    shard.start_transaction()
    updated_ssn, updated_state, updated_occupation = sample_data[0]
    shard.update(updated_ssn, f'{updated_state},Updated {updated_occupation}')
    shard.end_transaction()
    memory_layer.put(updated_ssn, f'{updated_state},Updated {updated_occupation}')

    # Retrieve updated values from memory layer
    print(f'{updated_ssn} -> {memory_layer.get(updated_ssn)}')  # Output: state,Updated occupation

    # Delete a key
    shard.start_transaction()
    deleted_ssn, deleted_state, deleted_occupation = sample_data[1]
    shard.delete(deleted_ssn)
    shard.end_transaction()
    memory_layer.put(deleted_ssn, None)

    # Retrieve deleted values from memory layer
    print(f'{deleted_ssn} -> {memory_layer.get(deleted_ssn)}')  # Output: None

    # Backup to S3
    shard.backup_to_s3()

    # Save memory layer
    memory_layer.save('memory_layer.parquet')

    # Load memory layer
    memory_layer.load('memory_layer.parquet')

    # Cleanup test files after the test
    for file1 in os.listdir(wal_dir):
        os.remove(os.path.join(wal_dir, file1))
    os.rmdir(wal_dir)
    if os.path.exists(data_file):
        os.remove(data_file)
    if os.path.exists(index_file):
        os.remove(index_file)


test_integration()
