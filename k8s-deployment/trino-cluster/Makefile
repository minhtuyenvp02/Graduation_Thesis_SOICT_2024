.PHONY: minio-trino
hive-meta-store:
	cd trino-minio
	helm upgrade --install hive-metastore-postgresql bitnami/postgresql -n trino -f hive-metastore-postgresql/values.yaml
	helm upgrade --install my-hive-metastore -n trino -f hive-metastore/values.yaml ./charts/hive-metastore
	helm upgrade --install my-redis bitnami/redis -n trino -f redis/values.yaml
redis:
	kubectl create ns trino
	kubectl create secret generic redis-table-definition --from-file=redis/test.json -n trino || true
trino:
	helm repo add bitnami https://charts.bitnami.com/bitnami || true
	helm repo add trino https://trinodb.github.io/charts/ || true
	helm upgrade --install my-trino trino/trino --namespace trino -f trino/values.yaml

minio-cluster:
	helm repo add bitnami https://charts.bitnami.com/bitnami
	helm repo update
    helm install minio oci://registry-1.docker.io/bitnamicharts/minio -n minio --create-namespace -f minio-values.yaml
clean-minio-cluster:
	helm delete minio --namespace minio
