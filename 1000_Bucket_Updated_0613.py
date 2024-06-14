import os

from faker import Faker
import pandas as pd
import random
import pyarrow
import hashlib
import os
from Hash_1000_Buckets import SSN

# initialize Faker
fake = Faker()

num_records = 1000000

# generate SSN set first for unique records then conver to list
ssns = set()
while len(ssns) < num_records:
    ssns.add(fake.ssn())
ssns = list(ssns)

# generate state list
state = [
    'AL', 'AK', 'AZ', 'AR', 'CA', 'CO', 'CT', 'DE', 'FL', 'GA', 'HI', 'ID',
    'IL', 'IN', 'IA', 'KS', 'KY', 'LA', 'ME', 'MD', 'MA', 'MI', 'MN',
    'MS', 'MO', 'MT', 'NE', 'NV', 'NH', 'NJ', 'NM', 'NY', 'NC', 'ND',
    'OH', 'OK', 'OR', 'PA', 'RI', 'SC', 'SD', 'TN', 'TX', 'UT', 'VT',
    'VA', 'WA', 'WV', 'WI', 'WY'
]
random_state = random.choices(state, k=num_records)

# occupation list
occupation = [
    'Accountant', 'Actor', 'Actuary', 'Administrative Assistant',
    'Aerospace Engineer', 'Agricultural Engineer', 'Air Traffic Controller',
    'Aircraft Mechanic', 'Airline Pilot', 'Anesthesiologist',
    'Anthropologist', 'Architect', 'Archivist', 'Art Director',
    'Astronomer', 'Athletic Trainer', 'Audiologist', 'Author',
    'Automotive Mechanic', 'Baker', 'Bartender', 'Biochemist',
    'Biomedical Engineer', 'Brickmason', 'Broadcast Technician'
]
random_occupation = random.choices(occupation, k=num_records)

# generate dataframe
df = pd.DataFrame({'SSN': ssns,
                   'State': random_state,
                   'Occupation': random_occupation
                   })

# create primary index with SSN
df.set_index('SSN', inplace=True)
print(df)

# create secondary index with State and Occupation
# approach 1 with dictionary
# state_index = df.groupby('State').apply(lambda x: x.index.tolist()).to_dict()
# print(state_index)

# occupation_index = df.groupby('Occupation').apply(lambda x: x.index.tolist()).to_dict()
# print(occupation_index)

# approach 2 with multiindex
df.set_index(['State', 'Occupation'], inplace=True, append=True, drop=False)
print(df)

# load data into parquet file
df.to_parquet('KV_Store.parquet', index=True)

# hash file into 1000 buckets with hashing function
df = pd.read_parquet('KV_Store.parquet')
print(df)


# make a directory for the 1000 buckets


# bucket using hashing function
def hash_index_to_bucket(ssn, num_buckets=1000):
    hash_object = hashlib.md5(str(ssn).encode())
    bucket = int(hash_object.hexdigest(), 16) % num_buckets
    return bucket


# Create a dictionary to hold data for each bucket
buckets = {i: [] for i in range(1000)}

# Distribute rows into buckets
for index, row in df.iterrows():
    bucket = hash_index_to_bucket(index[0])
# include index when distribution to each bucket
    buckets[bucket].append((index, row))


# Save each bucket to a Parquet file, including the index
output_dir = 'buckets'
os.makedirs(output_dir, exist_ok=True)
for bucket, data in buckets.items():
    if data:
        # Create a DataFrame from the list of tuples, preserving the index
        bucket_df = pd.DataFrame([row for _, row in data], columns=df.columns)
        bucket_df.index = pd.MultiIndex.from_tuples([index for index, _ in data], names=df.index.names)
        bucket_df.to_parquet(os.path.join(output_dir, f'bucket-{bucket}.parquet'), index=True)

print("file with index has been distributed to 1000 buckets")


# validation

# Define the directory where the buckets are saved
output_dir = 'buckets'

# Specify the bucket file you want to check
bucket_file = os.path.join(output_dir, 'bucket-1.parquet')

# Check if the file exists
if os.path.exists(bucket_file):
    # Read the Parquet file
    df = pd.read_parquet(bucket_file)

    # Print the DataFrame to verify the data and index
    print(df)
else:
    print(f"Bucket file {bucket_file} does not exist.")









