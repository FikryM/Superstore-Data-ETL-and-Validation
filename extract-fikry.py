import kagglehub
import os

DATASET_ROOT_DIR = "/opt/airflow/data"


path = kagglehub.dataset_download("vivek468/superstore-dataset-final")
print("Path to dataset files:", path)
if not os.path.exists(DATASET_ROOT_DIR):
    os.makedirs(DATASET_ROOT_DIR)
os.system("cp -r {}/* {}".format(path, DATASET_ROOT_DIR))
print("Path to dataset files:", path)
