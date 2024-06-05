.PHONY: spark-cluster

spark-cluster: # Initialize the spark-cluster
	helm repo add bitnami https://charts.bitnami.com/bitnami
	helm repo update
    helm install spark oci://registry-1.docker.io/bitnamicharts/spark -n spark --create-namespace -f spark-values.yaml

# spark-port-forward:
# 	kubectl port-forward --namespace spark-cluster svc/spark-master-svc 10001:80

clean-spark-cluster:
	helm uninstall spark --namespace spark
	kubectl delete namespace spark