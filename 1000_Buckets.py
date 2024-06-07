from faker import Faker
import pandas as pd
import random
import pyarrow
import hashlib

# initialize Faker
fake = Faker()

num_records = 100

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


def hash_to_bucket(ssn, num_buckets=1000):
    hash_value = int(hashlib.md5(ssn.encode()).hexdigest(), 16)
    return hash_value % num_buckets


# apply hash_to_bucket function to create a new column in df
df['Bucket'] = df.index.to_series().map(lambda ssn: hash_to_bucket(str(ssn)))

print(df.head())

# distribute the dataframe into 1000 buckets based on Bucket column
buckets = {i: df[df['Bucket'] == i] for i in range(1000)}

for i in range(1000):
    bucket_df = buckets[i]
    bucket_df.to_parquet(f'Bucket_{i}.parquet')


# verify file
# df_loaded = pd.read_parquet()