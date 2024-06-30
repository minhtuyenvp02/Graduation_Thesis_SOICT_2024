.PHONY: kafka-cluster

kafka-cluster:
	helm repo add bitnami https://charts.bitnami.com/bitnami
	helm repo update
	helm install kafka oci://registry-1.docker.io/bitnamicharts/kafka -n kafka --create-namespace -f kafka-values.yaml
	helm install kafka-metrics prometheus-community/prometheus-kafka-exporter -n kafka -f kafka-prome-exporter.yaml
update-kafka-cluster:
	helm upgrade --install kafka oci://registry-1.docker.io/bitnamicharts/kafka -n kafka -f kafka-values.yaml
clean-kafka-cluster:
	helm delete kafka --namespace kafka