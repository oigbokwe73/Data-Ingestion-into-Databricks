# Data-Ingestion-into-Databricks
Data Ingestion. Databricks Lakehouse


### Using TensorFlow with Parquet Files

First, ensure you have the necessary libraries installed:

```bash
pip install tensorflow pandas pyarrow
```

#### Example Workflow in TensorFlow

1. **Reading Parquet Files:**
   Use `pandas` to read the Parquet file into a DataFrame.
   
2. **Converting DataFrame to TensorFlow Dataset:**
   Convert the pandas DataFrame to a TensorFlow Dataset for further processing and training.

```python
import pandas as pd
import tensorflow as tf

# Step 1: Read Parquet file into a DataFrame
df = pd.read_parquet('data.parquet')

# Step 2: Convert DataFrame to TensorFlow Dataset
def df_to_dataset(dataframe, shuffle=True, batch_size=32):
    dataframe = dataframe.copy()
    labels = dataframe.pop('target')  # assuming 'target' is the label column
    ds = tf.data.Dataset.from_tensor_slices((dict(dataframe), labels))
    if shuffle:
        ds = ds.shuffle(buffer_size=len(dataframe))
    ds = ds.batch(batch_size)
    return ds

batch_size = 32
dataset = df_to_dataset(df, batch_size=batch_size)

# Step 3: Use the dataset for model training
model = tf.keras.models.Sequential([
    tf.keras.layers.Dense(64, activation='relu'),
    tf.keras.layers.Dense(1)  # assuming a regression task
])

model.compile(optimizer='adam', loss='mean_squared_error')
model.fit(dataset, epochs=10)
```

### Using PyTorch with Parquet Files

First, ensure you have the necessary libraries installed:

```bash
pip install torch pandas pyarrow
```

#### Example Workflow in PyTorch

1. **Reading Parquet Files:**
   Use `pandas` to read the Parquet file into a DataFrame.
   
2. **Creating a PyTorch Dataset:**
   Convert the DataFrame into a PyTorch Dataset.

```python
import pandas as pd
import torch
from torch.utils.data import Dataset, DataLoader

# Step 1: Read Parquet file into a DataFrame
df = pd.read_parquet('data.parquet')

# Step 2: Create a custom PyTorch Dataset
class ParquetDataset(Dataset):
    def __init__(self, dataframe):
        self.dataframe = dataframe
        self.features = dataframe.drop('target', axis=1).values  # assuming 'target' is the label column
        self.labels = dataframe['target'].values

    def __len__(self):
        return len(self.dataframe)

    def __getitem__(self, idx):
        features = torch.tensor(self.features[idx], dtype=torch.float32)
        label = torch.tensor(self.labels[idx], dtype=torch.float32)
        return features, label

# Step 3: Create DataLoader
batch_size = 32
dataset = ParquetDataset(df)
dataloader = DataLoader(dataset, batch_size=batch_size, shuffle=True)

# Step 4: Define a simple model
model = torch.nn.Sequential(
    torch.nn.Linear(df.shape[1] - 1, 64),  # number of features minus label
    torch.nn.ReLU(),
    torch.nn.Linear(64, 1)  # assuming a regression task
)

# Step 5: Define loss function and optimizer
criterion = torch.nn.MSELoss()
optimizer = torch.optim.Adam(model.parameters(), lr=0.001)

# Step 6: Training loop
for epoch in range(10):  # number of epochs
    for features, labels in dataloader:
        optimizer.zero_grad()
        outputs = model(features)
        loss = criterion(outputs, labels.view(-1, 1))
        loss.backward()
        optimizer.step()
    print(f'Epoch {epoch+1}, Loss: {loss.item()}')
```

### Conclusion

Both TensorFlow and PyTorch can efficiently work with Parquet files by using `pandas` to handle the file reading and then converting the DataFrame into datasets compatible with each framework. This allows for easy integration of big data storage formats with deep learning frameworks. 
