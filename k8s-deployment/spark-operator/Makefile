.PHONY: spark-cluster
spark-operator:
	helm repo add spark-operator https://kubeflow.github.io/spark-operator
	helm repo update
	helm install spark spark-operator/spark-operator --namespace spark --create-namespace -f spark-operator-values.yaml
	kubectl apply -f spark-env-secret.yaml
    kubectl create serviceaccount spark --namespace spark
	kubectl create clusterrolebinding spark-role --clusterrole=edit --serviceaccount=spark:spark --namespace=spark
	kubectl create clusterrolebinding default-admin --clusterrole cluster-admin --serviceaccount=airflow:airflow-worker --namespace spark
upgrade-spark-operator:
	helm upgrade --install spark spark-operator/spark-operator --namespace spark -f spark-operator-values.yaml
clean:
	kubectl delete ns spark
delete-unused-svc:
	kubectl get services -n spark -o name | grep "to-bronze" | xargs kubectl delete -n spark

	kubectl get services -n spark -o name | grep "scd0" | xargs kubectl delete -n spark

	 kubectl delete pod --field-selector=status.phase==Failed -n spark
