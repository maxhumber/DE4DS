Convert the model to a script:

```
jupyter nbconvert --to script model.ipynb
```





```python
python -m venv env
source env/bin/activate
pip install jupyter
pip install pandas
pip freeze > requirements.txt
pip install apache-airflow
```



