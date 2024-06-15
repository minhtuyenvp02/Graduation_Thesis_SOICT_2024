.PHONY: airflow
airflow:
	helm repo add apache-airflow https://airflow.apache.org
	helm upgrade --install airflow apache-airflow/airflow --namespace airflow --create-namespace -f airflow-values.yaml --timeout=40m
	kubectl create secret generic webserver-secret --from-literal="webserver-secret-key=$(python3 -c 'import secrets; print(secrets.token_hex(16))')" -n airflow
update-airflow:
	helm upgrade --install airflow apache-airflow/airflow --namespace airflow -f airflow-values.yaml
	make airflow
clean-airflow:
	helm uninstall airflow --namespace airflow
	kubectl delete namespace airflow
	kubectl delete pod --field-selector=status.phase==Failed -n airflow
