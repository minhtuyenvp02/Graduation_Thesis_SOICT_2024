.PHONY: kafka-cluster

kafka-cluster:
	helm repo add bitnami https://charts.bitnami.com/bitnami
	helm repo update
	helm install kafka oci://registry-1.docker.io/bitnamicharts/kafka -n kafka --create-namespace -f kafka-values.yaml

# helm repo add confluentinc https://packages.confluent.io/helm
# helm repo update
# helm upgrade --install confluent-operator confluentinc/confluent-for-kubernetes --namespace kafka --create-namespace
update-kafka-cluster:
	helm upgrade --install kafka oci://registry-1.docker.io/bitnamicharts/kafka -n kafka -f kafka-values.yaml
clean-kafka-cluster:
	helm delete kafka --namespace kafka