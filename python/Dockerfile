FROM python:3.9.10-bullseye
COPY jupyter /etc/jupyter
RUN pip install --upgrade pip
RUN pip install -r /etc/jupyter/requirements.txt
RUN sed -i -e 's/Python 3 (ipykernel)/Python 3.9.10/g' /usr/local/share/jupyter/kernels/python3/kernel.json
CMD ["jupyter", "lab","--config", "/etc/jupyter/jupyter_lab_config.py"]
