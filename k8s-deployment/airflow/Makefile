.PHONY: airflow
airflow:
	helm repo add apache-airflow https://airflow.apache.org
	helm upgrade --install airflow apache-airflow/airflow --namespace airflow --create-namespace -f airflow-values.yaml --timeout=40m
update-airflow:
	helm uninstall airflow --namespace airflow
	make airflow
clean-airflow:
	helm uninstall airflow --namespace airflow
	kubectl delete namespace airflow
	kubectl delete pod --field-selector=status.phase==Failed -n airflow
