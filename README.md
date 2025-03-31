# Build image
```shell
docker build -t tabnet_jupyter_gpu .
```

# Run Linux
```shell
docker run --gpus all -it --rm -p 8888:8888 -v $(pwd):/workspace tabnet_jupyter_gp
```

# Run Windows
```shell
docker run --gpus all -it --rm -p 8888:8888 -v ${PWD}:/workspace tabnet_jupyter_gpu
```

# Dataset source
https://data.binance.vision/?prefix=data/

# Run to download binance btc spot archive 
```shell
go run main.go
```

# Train TabNet on large dataset
```python
from sklearn.model_selection import train_test_split
from pytorch_tabnet.tab_model import TabNetClassifier
import pandas as pd

df = pd.read_csv('large_dataset_normalized.csv')

df.drop('month', inplace=True, axis=1) 

X = df.drop(columns=['target'])
y = df['target']

X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, shuffle=False)

clf = TabNetClassifier(
    device_name="cuda",  # Force GPU usage
    optimizer_fn=torch.optim.AdamW,  # AdamW performs better on large data
)
clf.fit(
    X_train.values, y_train.values,
    eval_set=[(X_test.values, y_test.values)],
    max_epochs=100,
    patience=20,
    batch_size=32768,  # 🔥 Use large batch sizes since RAM is not a limit
    virtual_batch_size=32768,  # 🔥 Increase to use more GPU
    num_workers=6,  # 🔥 Keep lower as data is in RAM
    pin_memory=True,  # 🔥 Faster CPU → GPU memory transfers
)
```