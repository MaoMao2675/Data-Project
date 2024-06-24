import json
import os
import datetime
import pandas as pd
import hashlib
import json


class WriteAheadLog:
    def __init__(self, wal_dir='./wal_v3', max_wal_operations=100):
        self.wal_dir = wal_dir
        self.max_wal_operations = max_wal_operations
        self.wal_counter = 0
        self.wal_file_counter = 0
        self.current_transaction = []
        os.makedirs(wal_dir, exist_ok=True)

    def get_current_wal_file(self):
        wal_file_path = os.path.join(self.wal_dir, f'wal_{self.wal_file_counter}.wal')
        if not os.path.exists(wal_file_path):
            open(wal_file_path, 'a').close()  # Create an empty file if it does not exist
        return wal_file_path

    def rotate_wal_file(self):
        self.wal_counter = 0
        self.wal_file_counter += 1

    def log_to_wal(self, operation, data, is_rollback=False):
        timestamp = datetime.datetime.now().isoformat()
        wal_entry = {'timestamp': timestamp, 'operation': operation, 'data': data, 'is_rollback': is_rollback}
        wal_file_path = self.get_current_wal_file()
        with open(wal_file_path, 'a') as wal_file:
            wal_file.write(json.dumps(wal_entry) + '\n')
        self.wal_counter += 1
        if self.wal_counter >= self.max_wal_operations:
            self.rotate_wal_file()

    def start_transaction(self):
        self.current_transaction = []
        self.log_to_wal('start_transaction', {}, is_rollback=False)

    def end_transaction(self):
        self.log_to_wal('end_transaction', {}, is_rollback=False)
        self.current_transaction = []

    def log_operation(self, operation, data, is_rollback=False):
        self.log_to_wal(operation, data, is_rollback)
        if not is_rollback:
            self.current_transaction.append({'operation': operation, 'data': data})

    def load_wal(self):
        wal_files = sorted([f for f in os.listdir(self.wal_dir) if f.endswith('.wal')])
        transactions = []
        for wal_file in wal_files:
            with open(os.path.join(self.wal_dir, wal_file), 'r') as file:
                for line in file:
                    transactions.append(json.loads(line.strip()))
        return transactions

    def clear_wal(self):
        wal_files = [f for f in os.listdir(self.wal_dir) if f.endswith('.wal')]
        for wal_file in wal_files:
            os.remove(os.path.join(self.wal_dir, wal_file))

    def rollback_transaction(self, entry, crud):
        operation = entry['operation']
        data = entry['data']
        if operation == 'create':
            crud.delete_record(data['SSN'], rollback=True)
        elif operation == 'update':
            old_data = data['old_data']
            crud.update_record(data['SSN'], old_data, rollback=True)
        elif operation == 'delete':
            crud.create_record(data, rollback=True)

    def process_wal(self, crud):
        wal_files = sorted([f for f in os.listdir(self.wal_dir) if f.startswith('wal_')])
        for wal_file in wal_files:
            wal_file_path = os.path.join(self.wal_dir, wal_file)
            with open(wal_file_path, 'r') as file:
                for line in file:
                    entry = json.loads(line.strip())
                    operation = entry['operation']
                    data = entry['data']
                    if operation == 'create':
                        crud.delete_record(data['SSN'], rollback=True)
                    elif operation == 'update':
                        crud.update_record(data['SSN'], data['old_data'], rollback=True)
                    elif operation == 'delete':
                        crud.create_record(data, rollback=True)
            os.remove(wal_file_path)


class CRUDOperations:
    def __init__(self, buckets_dir='./buckets_v5', wal=None):
        self.buckets_dir = buckets_dir
        self.wal = wal
        os.makedirs(buckets_dir, exist_ok=True)

    def hash_index_to_bucket(self, ssn, num_buckets=1000):
        hash_object = hashlib.md5(str(ssn).encode())
        bucket = int(hash_object.hexdigest(), 16) % num_buckets
        return bucket

    def update_secondary_index(self, index_file_path, record, index_key, old_value=None, remove=False):
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

    def create_record(self, record, rollback=False):
        ssn = record['SSN']
        bucket = self.hash_index_to_bucket(ssn)
        bucket_file_path = os.path.join(self.buckets_dir, f'bucket_{bucket}.parquet')
        state_index_file_path = os.path.join(self.buckets_dir, f'state_index_{bucket}.json')
        occupation_index_file_path = os.path.join(self.buckets_dir, f'occupation_index_{bucket}.json')

        if not rollback:
            self.wal.log_operation('create', record)

        try:
            # Load existing data if the file exists
            if os.path.exists(bucket_file_path):
                bucket_df = pd.read_parquet(bucket_file_path)
                print(f"Loaded existing data from {bucket_file_path}:\n{bucket_df}")
            else:
                bucket_df = pd.DataFrame(columns=['SSN', 'State', 'Occupation'])
                print(f"No existing data found, creating new DataFrame")

            # Check if the record already exists
            if ssn in bucket_df.index:
                print(f"Record with SSN {ssn} already exists.")
                return

            # Ensure the new record is in the correct format
            new_record_df = pd.DataFrame([record])
            new_record_df.set_index('SSN', inplace=True, drop=False)
            print(f"New record DataFrame:\n{new_record_df}")

            # Concatenate with proper alignment
            bucket_df = pd.concat([bucket_df, new_record_df])
            print(f"Concatenated DataFrame:\n{bucket_df}")

            # Save the updated DataFrame back to the Parquet file
            bucket_df.sort_index(inplace=True)
            bucket_df.to_parquet(bucket_file_path, index=True)
            print(f"Saved updated DataFrame to {bucket_file_path}")

            # Update secondary indexes
            self.update_secondary_index(state_index_file_path, record, 'State')
            self.update_secondary_index(occupation_index_file_path, record, 'Occupation')
        except Exception as e:
            print(f"Create operation failed: {e}")
            if not rollback:
                for entry in reversed(self.wal.current_transaction):
                    self.wal.rollback_transaction(entry, self)
                self.wal.clear_wal()

    def read_record(self, ssn):
        bucket = self.hash_index_to_bucket(ssn)
        bucket_file_path = os.path.join(self.buckets_dir, f'bucket_{bucket}.parquet')

        if os.path.exists(bucket_file_path):
            bucket_df = pd.read_parquet(bucket_file_path)
            if ssn in bucket_df.index:
                record = bucket_df.loc[ssn].to_dict()
                record = {'SSN': ssn, **record}  # Ensure SSN is included in the returned record
                return record

        print(f"Record with SSN {ssn} not found.")
        return None

    def update_record(self, ssn, updates, rollback=False):
        bucket = self.hash_index_to_bucket(ssn)
        bucket_file_path = os.path.join(self.buckets_dir, f'bucket_{bucket}.parquet')
        state_index_file_path = os.path.join(self.buckets_dir, f'state_index_{bucket}.json')
        occupation_index_file_path = os.path.join(self.buckets_dir, f'occupation_index_{bucket}.json')

        try:
            if os.path.exists(bucket_file_path):
                bucket_df = pd.read_parquet(bucket_file_path)
                print(f"Bucket DataFrame before update:\n{bucket_df}")
                if ssn in bucket_df.index:
                    old_record = bucket_df.loc[ssn].to_dict()
                    new_record = {**old_record, **updates}
                    if not rollback:
                        self.wal.log_operation('update', {'SSN': ssn, 'old_data': old_record, 'new_data': new_record})

                    # Update the record in the DataFrame
                    for key, value in updates.items():
                        bucket_df.at[ssn, key] = value

                    print(f"Bucket DataFrame after update:\n{bucket_df}")

                    bucket_df = bucket_df.sort_index()
                    bucket_df.to_parquet(bucket_file_path, index=True)

                    # Update secondary indexes if necessary
                    if 'State' in updates:
                        self.update_secondary_index(state_index_file_path, new_record, 'State',
                                                    old_value=old_record['State'])
                    if 'Occupation' in updates:
                        self.update_secondary_index(occupation_index_file_path, new_record, 'Occupation',
                                                    old_value=old_record['Occupation'])
                else:
                    print(f"Record with SSN {ssn} not found.")
            else:
                print(f"Bucket file for SSN {ssn} not found.")
        except Exception as e:
            print(f"Update operation failed: {e}")
            if not rollback:
                for entry in reversed(self.wal.current_transaction):
                    self.wal.rollback_transaction(entry, self)
                self.wal.clear_wal()

    def delete_record(self, ssn, rollback=False):
        bucket = self.hash_index_to_bucket(ssn)
        bucket_file_path = os.path.join(self.buckets_dir, f'bucket_{bucket}.parquet')
        state_index_file_path = os.path.join(self.buckets_dir, f'state_index_{bucket}.json')
        occupation_index_file_path = os.path.join(self.buckets_dir, f'occupation_index_{bucket}.json')

        try:
            if os.path.exists(bucket_file_path):
                bucket_df = pd.read_parquet(bucket_file_path)
                if ssn in bucket_df.index:
                    old_record = bucket_df.loc[ssn].to_dict()

                    self.wal.log_operation('delete', old_record, is_rollback=rollback)

                    bucket_df = bucket_df.drop(index=ssn)
                    bucket_df.to_parquet(bucket_file_path, index=True)

                    self.update_secondary_index(state_index_file_path, old_record, 'State', remove=True)
                    self.update_secondary_index(occupation_index_file_path, old_record, 'Occupation', remove=True)
                else:
                    print(f"Record with SSN {ssn} not found.")
            else:
                print(f"Bucket file for SSN {ssn} not found.")
        except Exception as e:
            print(f"Delete operation failed: {e}")
            if not rollback:
                for entry in reversed(self.wal.current_transaction):
                    self.wal.rollback_transaction(entry, self)
                self.wal.clear_wal()


def test_crud_operations():
    # Initialize WAL and CRUD operations
    wal = WriteAheadLog()
    crud = CRUDOperations(wal=wal)

    # Start a transaction
    wal.start_transaction()

    # Create records
    crud.create_record({'SSN': '111-00-0000', 'State': 'CA', 'Occupation': 'Engineer'})

    # End the transaction
    wal.end_transaction()

    # Verify records were created
    print(crud.read_record('111-00-0000'))

    # Update a record
    wal.start_transaction()
    crud.update_record('111-00-0000', {'Occupation': 'Dentist'})
    wal.end_transaction()

    # Verify the record was updated
    print(crud.read_record('111-00-0000'))


if __name__ == "__main__":
    test_crud_operations()
