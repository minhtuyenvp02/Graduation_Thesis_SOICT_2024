.PHONY: minio-trino
trino:
	kubectl create secret generic redis-table-definition --from-file=redis/test.json -n trino || true
	helm repo add bitnami https://charts.bitnami.com/bitnami || true
	helm repo add trino https://trinodb.github.io/charts/ || true
hive-meta-store:

minio-cluster:
	helm repo add bitnami https://charts.bitnami.com/bitnami
	helm repo update
    helm install minio oci://registry-1.docker.io/bitnamicharts/minio -n minio --create-namespace -f minio-values.yaml
clean-minio-cluster:
	helm delete minio --namespace minio
