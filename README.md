<h1>Preprocessing with Python shell and PySpark AWS Glue jobs</h1>

| Owners | Maria Cortes |
| ------ | ------ |
| Date| May, 2021 |

<h2>Objective</h2>

This artifact provides two approaches leveraging AWS Glue Jobs: Python Shell and PySpark. The artifact is organized as follows:

```
glue_preprocessing/
├── readme.md          <- This document
├── requirements.txt <- Text file with Python libraries to install
├── notebooks
│   ├── 1. Example Glue Processing with Python.ipynb     <- Preprocessing using a Python script in a Python shell Glue job
│   ├── 2. Example Glue Processing with PySpark.ipynb    <- Preprocessing using a PySpark script
├── src
│   ├── glue_pyhton_script.py <- Script for Python Glue job
│   ├── glue_pyspark_script.py <- Script for PySpark Glue job
``` 

The steps below outline the procedure for running the two Glue jobs.

<h2> Step 1. Running 1. Example Glue Processing with Python.ipynb Jupyter notebook </h2>

The first Jupyter Notebook provides guidance in using a Python script in a Python shell Glue job to run a small data processing workflow. It also provides guidance on how to install additional Python libraries. Run this notebook and make sure that the Glue job succeeds. 

<h2> Step 2. Running 2. Example Glue Processing with PySpark.ipynb Jupyter notebook </h2>

The second Jupyter Notebook uses the PySpark Python dialect for running an AWS Glue job, and provides guidance on how to include additional Python modules. Run this notebook and make sure that the Glue job succeeds.