FROM nvcr.io/nvidia/pytorch:25.03-py3

RUN pip install --upgrade pip
RUN pip install \
    pytorch-tabnet \
    pandas \
    pandas_ta \
    numpy \
    matplotlib \
    scikit-learn \
    optuna \
    jupyter

WORKDIR /workspace

CMD ["jupyter", "notebook", "--ip=0.0.0.0", "--port=8888", "--no-browser", "--allow-root"]
