spark-operator:
	helm repo add spark-operator https://kubeflow.github.io/spark-operator
	helm repo update
	helm install spark-operator spark-operator/spark-operator \
    --namespace spark --create-namespace -f spark-operator-values.yaml

upgrade-spark-operator:
	helm upgrade --install spark-operator spark-operator/spark-operator \
    --namespace spark -f spark-operator-values.yaml
delete-unused-svc:
	kubectl get services -n spark -o name | grep "data-to-bronze" | xargs kubectl delete -n spark

	kubectl get services -n spark -o name | grep "data-to-bronze" | xargs kubectl delete -n spark
