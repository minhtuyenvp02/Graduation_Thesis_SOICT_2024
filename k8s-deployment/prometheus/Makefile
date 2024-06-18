prometheus-cluster:
	helm install prometheus prometheus-community/kube-prometheus-stack -n prometheus --create-namespace -f prometheus-values.yaml
upgrade:
	helm upgrade  --install prometheus prometheus-community/kube-prometheus-stack -n prometheus -f prometheus-values.yaml