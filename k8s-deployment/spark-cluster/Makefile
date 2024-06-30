.PHONY: spark-cluster

spark-cluster: # Initialize the spark-cluster
	helm repo add bitnami https://charts.bitnami.com/bitnami
	helm repo update
    helm install spark oci://registry-1.docker.io/bitnamicharts/spark -n spark --create-namespace -f spark-values.yaml
	kubectl create serviceaccount spark --namespace=spark
	kubectl create clusterrolebinding spark-role --clusterrole=edit --serviceaccount=spark:spark --namespace=spark
clean-spark-cluster:
	helm uninstall spark --namespace spark
	kubectl delete namespace spark