.PHONY: minio-cluster

minio-cluster:
	helm repo add bitnami https://charts.bitnami.com/bitnami
	helm repo update
    helm install minio oci://registry-1.docker.io/bitnamicharts/minio -n minio --create-namespace -f minio-values.yaml

# helm repo add confluentinc https://packages.confluent.io/helm
# helm repo update
# helm upgrade --install confluent-operator confluentinc/confluent-for-kubernetes --namespace kafka --create-namespace
update-minio-cluster:
	    helm upgrade --install minio oci://registry-1.docker.io/bitnamicharts/minio -n minio -f minio-values.yaml
clean-minio-cluster:
	helm delete minio --namespace minio