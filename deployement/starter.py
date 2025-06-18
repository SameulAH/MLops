# #!/usr/bin/env python
# # coding: utf-8

# # In[1]:


# get_ipython().system('pip freeze | grep scikit-learn')


# # In[2]:


# get_ipython().system('python -V')


# # In[12]:


import pickle
import pandas as pd
import numpy as np


# In[4]:


with open('model.bin', 'rb') as f_in:
    dv, model = pickle.load(f_in)


# In[7]:


categorical = ['PULocationID', 'DOLocationID']

def read_data(filename):
    df = pd.read_parquet(filename)

    df['duration'] = df.tpep_dropoff_datetime - df.tpep_pickup_datetime
    df['duration'] = df.duration.dt.total_seconds() / 60

    df = df[(df.duration >= 1) & (df.duration <= 60)].copy()

    df[categorical] = df[categorical].fillna(-1).astype('int').astype('str')

    return df


# In[ ]:


df = read_data('https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-03.parquet')


# In[10]:


dicts = df[categorical].to_dict(orient='records')
X_val = dv.transform(dicts)
y_pred = model.predict(X_val)


# In[ ]:





# 
# ## Q1. Notebook
# 
# We'll start with the same notebook we ended up with in homework 1.
# We cleaned it a little bit and kept only the scoring part. You can find the initial notebook [here](homework/starter.ipynb).
# 
# Run this notebook for the March 2023 data.
# 
# What's the standard deviation of the predicted duration for this dataset?
# 

# In[14]:


std_dev = np.std(y_pred)
print(std_dev)


# ## Q2. Preparing the output
# 
# Like in the course videos, we want to prepare the dataframe with the output. 
# 
# First, let's create an artificial `ride_id` column:

# In[16]:


df.columns


# In[17]:


df['prediction'] = y_pred
df['year'] = df.tpep_pickup_datetime.dt.year
df['month'] = df.tpep_pickup_datetime.dt.month
df['ride_id'] = df['year'].astype(str).str.zfill(4) + '/' + df['month'].astype(str).str.zfill(2) + '_' + df.index.astype('str')


# In[18]:


output_file = 'result.parquet'


# In[19]:


df_result = df[['ride_id', 'prediction']].copy()
df_result.to_parquet(
        output_file,
        engine='pyarrow',
        compression=None,
        index=False
    )
print(f"Predictions saved to {output_file}")
print(f"Mean predicted duration: {np.mean(y_pred)}") 


# In[22]:


import os

size_bytes = os.path.getsize(output_file)
size_MB = size_bytes / (1024 * 1024)
print(f'Size of the output file: {size_MB:.2f} MB')


# ## Q3. Creating the scoring script
# 
# Now let's turn the notebook into a script. 
# 
# Which command you need to execute for that?

# In[ ]:


# Turn notebook to script
# get_ipython().system('jupyter nbconvert --to=script starter.ipynb')


# ## Q4. Virtual environment
# 
# Now let's put everything into a virtual environment. We'll use pipenv for that.
# 
# Install all the required libraries. Pay attention to the Scikit-Learn version: it should be the same as in the starter
# notebook.
# 
# After installing the libraries, pipenv creates two files: `Pipfile`
# and `Pipfile.lock`. The `Pipfile.lock` file keeps the hashes of the
# dependencies we use for the virtual env.
# 
# What's the first hash for the Scikit-Learn dependency?

# In[ ]:




