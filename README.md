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