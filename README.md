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

# Run Local Jupyter
http://127.0.0.1:8888/tree