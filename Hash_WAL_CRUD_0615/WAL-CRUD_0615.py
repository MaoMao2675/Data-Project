import json
import os
import datetime
import pandas as pd
import hashlib

BUCKETS_DIR = './buckets_v4'
WAL_DIR = './wal_v2'
MAX_WAL_OPERATIONS = 100

# Global variables for WAL
wal_counter = 0
wal_file_counter = 0
current_transaction = []

# Initialize WAL directory
os.makedirs(WAL_DIR, exist_ok=True)


# Define WAL functions with transactions
def get_current_wal_file():
    global wal_file_counter
    wal_file_path = os.path.join(WAL_DIR, f'wal_{wal_file_counter}.wal')
    if not os.path.exists(wal_file_path):
        open(wal_file_path, 'a').close()  # Create an empty file if it does not exist
    return wal_file_path


def rotate_wal_file():
    global wal_counter, wal_file_counter
    wal_counter = 0
    wal_file_counter += 1


def log_to_wal(operation, data):
    global wal_counter
    timestamp = datetime.datetime.now().isoformat()
    wal_entry = {'timestamp': timestamp, 'operation': operation, 'data': data}
    wal_file_path = get_current_wal_file()
    with open(wal_file_path, 'a') as wal_file:
        wal_file.write(json.dumps(wal_entry) + '\n')
    wal_counter += 1
    if wal_counter >= MAX_WAL_OPERATIONS:
        rotate_wal_file()


def start_transaction():
    global current_transaction
    current_transaction = []
    log_to_wal('start_transaction', {})


def end_transaction():
    global current_transaction
    log_to_wal('end_transaction', {})
    current_transaction = []


def log_operation(operation, data):
    global current_transaction
    log_to_wal(operation, data)
    current_transaction.append({'operation': operation, 'data': data})


def load_wal():
    wal_files = sorted([f for f in os.listdir(WAL_DIR) if f.endswith('.wal')])
    transactions = []
    for wal_file in wal_files:
        with open(os.path.join(WAL_DIR, wal_file), 'r') as file:
            for line in file:
                transactions.append(json.loads(line.strip()))
    return transactions


def clear_wal():
    wal_files = [f for f in os.listdir(WAL_DIR) if f.endswith('.wal')]
    for wal_file in wal_files:
        os.remove(os.path.join(WAL_DIR, wal_file))


# roll back function to revert a single operation when it fails
def rollback_transaction(entry):
    operation = entry['operation']
    data = entry['data']
    if operation == 'create':
        delete_record(data['SSN'], rollback=True)
    elif operation == 'update':
        old_data = data['old_data']
        update_record(data['SSN'], old_data, rollback=True)
    elif operation == 'delete':
        create_record(data, rollback=True)


# process the entire WAL to ensure the system starts in a consistent state

def process_wal():
    wal_files = sorted([f for f in os.listdir(WAL_DIR) if f.startswith('wal_')])
    for wal_file in wal_files:
        wal_file_path = os.path.join(WAL_DIR, wal_file)
        with open(wal_file_path, 'r') as file:
            for line in file:
                timestamp, operation, data = line.strip().split(',', 2)
                data = json.loads(data)
                if operation == 'create':
                    delete_record(data['SSN'], rollback=True)
                elif operation == 'update':
                    update_record(data['SSN'], data['old_data'], rollback=True)
                elif operation == 'delete':
                    create_record(data, rollback=True)
        os.remove(wal_file_path)


# Utility functions of hashing and updating secondary indexes when CRUD

def hash_index_to_bucket(ssn, num_buckets=1000):
    hash_object = hashlib.md5(str(ssn).encode())
    bucket = int(hash_object.hexdigest(), 16) % num_buckets
    return bucket


def update_secondary_index(index_file_path, record, index_key, old_value=None, remove=False):
    if os.path.exists(index_file_path):
        with open(index_file_path, 'r') as f:
            index_data = json.load(f)
    else:
        index_data = {}

    key_value = record[index_key]
    ssn = record['SSN']

    if remove:
        if old_value in index_data and ssn in index_data[old_value]:
            index_data[old_value].remove(ssn)
            if not index_data[old_value]:
                del index_data[old_value]
    else:
        if old_value and old_value != key_value:
            if old_value in index_data and ssn in index_data[old_value]:
                index_data[old_value].remove(ssn)
                if not index_data[old_value]:
                    del index_data[old_value]

        if key_value not in index_data:
            index_data[key_value] = []
        index_data[key_value].append(ssn)

    with open(index_file_path, 'w') as f:
        json.dump(index_data, f)


# CRUD functions
def create_record(record, rollback=False):
    ssn = record['SSN']
    bucket = hash_index_to_bucket(ssn)
    bucket_file_path = os.path.join(BUCKETS_DIR, f'bucket_{bucket}.parquet')
    state_index_file_path = os.path.join(BUCKETS_DIR, f'state_index_{bucket}.json')
    occupation_index_file_path = os.path.join(BUCKETS_DIR, f'occupation_index_{bucket}.json')

    if not rollback:
        log_operation('create', record)

    try:
        if os.path.exists(bucket_file_path):
            bucket_df = pd.read_parquet(bucket_file_path)
            if ssn in bucket_df.index:
                print(f"Record with SSN {ssn} already exists.")
                return
            new_record_df = pd.DataFrame([record]).set_index('SSN')
            bucket_df = pd.concat([bucket_df, new_record_df])
        else:
            bucket_df = pd.DataFrame([record]).set_index('SSN')

        bucket_df = bucket_df.sort_index()
        bucket_df.to_parquet(bucket_file_path, index=True)

        update_secondary_index(state_index_file_path, record, 'State')
        update_secondary_index(occupation_index_file_path, record, 'Occupation')
    except Exception as e:
        print(f"Create operation failed: {e}")
        if not rollback:
            for entry in reversed(current_transaction):
                rollback_transaction(entry)
            clear_wal()


def read_record(ssn):
    bucket = hash_index_to_bucket(ssn)
    bucket_file_path = os.path.join(BUCKETS_DIR, f'bucket_{bucket}.parquet')

    if os.path.exists(bucket_file_path):
        bucket_df = pd.read_parquet(bucket_file_path)
        if ssn in bucket_df.index:
            record = bucket_df.loc[ssn].to_dict()
            record['SSN'] = ssn  # Ensure SSN is included in the returned record
            return record

    print(f"Record with SSN {ssn} not found.")
    return None


def update_record(ssn, updates, rollback=False):
    bucket = hash_index_to_bucket(ssn)
    bucket_file_path = os.path.join(BUCKETS_DIR, f'bucket_{bucket}.parquet')
    state_index_file_path = os.path.join(BUCKETS_DIR, f'state_index_{bucket}.json')
    occupation_index_file_path = os.path.join(BUCKETS_DIR, f'occupation_index_{bucket}.json')

    try:
        if os.path.exists(bucket_file_path):
            bucket_df = pd.read_parquet(bucket_file_path)
            if ssn in bucket_df.index:
                old_record = bucket_df.loc[ssn].to_dict()   # get the old value
                new_record = {**old_record, **updates}

                if not rollback:
                    log_operation('update', {'SSN': ssn, 'old_data': old_record, 'new_data': new_record})   # log the old value and new value to WAL before actual updating

                # Update the record in the DataFrame
                for key, value in updates.items():
                    bucket_df.at[ssn, key] = value

                bucket_df = bucket_df.sort_index()
                bucket_df.to_parquet(bucket_file_path, index=True)

                # Update secondary indexes if necessary
                if 'State' in updates:
                    update_secondary_index(state_index_file_path, new_record, 'State', old_value=old_record['State'])
                if 'Occupation' in updates:
                    update_secondary_index(occupation_index_file_path, new_record, 'Occupation',
                                           old_value=old_record['Occupation'])
            else:
                print(f"Record with SSN {ssn} not found.")
        else:
            print(f"Bucket file for SSN {ssn} not found.")
    except Exception as e:
        print(f"Update operation failed: {e}")
        if not rollback:
            for entry in reversed(current_transaction):
                rollback_transaction(entry)
            clear_wal()


def delete_record(ssn, rollback=False):
    bucket = hash_index_to_bucket(ssn)
    bucket_file_path = os.path.join(BUCKETS_DIR, f'bucket_{bucket}.parquet')
    state_index_file_path = os.path.join(BUCKETS_DIR, f'state_index_{bucket}.json')
    occupation_index_file_path = os.path.join(BUCKETS_DIR, f'occupation_index_{bucket}.json')

    try:
        if os.path.exists(bucket_file_path):
            bucket_df = pd.read_parquet(bucket_file_path)
            if ssn in bucket_df.index:
                old_record = bucket_df.loc[ssn].to_dict()   # get the old value

                if not rollback:
                    log_operation('delete', old_record)   # log old value to WAL before actual deletion

                bucket_df = bucket_df.drop(index=ssn)
                bucket_df.to_parquet(bucket_file_path, index=True)

                update_secondary_index(state_index_file_path, old_record, 'State', remove=True)
                update_secondary_index(occupation_index_file_path, old_record, 'Occupation', remove=True)
            else:
                print(f"Record with SSN {ssn} not found.")
        else:
            print(f"Bucket file for SSN {ssn} not found.")
    except Exception as e:
        print(f"Delete operation failed: {e}")
        if not rollback:
            for entry in reversed(current_transaction):
                rollback_transaction(entry)
            clear_wal()


# start transaction
def initialize_system():
    process_wal()


try:
    start_transaction()
    create_record({'SSN': '888-45-6789', 'State': 'CA', 'Occupation': 'Engineer'})


    end_transaction()

except Exception as e:
    print(f"Transaction failed: {e}")
    for entry in reversed(current_transaction):
        rollback_transaction(entry)
    clear_wal()

print(read_record('888-45-6789'))


update_record('888-45-6789', {'Occupation': 'Dentist'}, rollback=False)
print(read_record('888-45-6789'))

