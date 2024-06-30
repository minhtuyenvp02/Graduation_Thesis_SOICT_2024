.PHONY: minio-cluster

minio-cluster:
	helm repo add bitnami https://charts.bitnami.com/bitnami
	helm repo update
    helm install minio oci://registry-1.docker.io/bitnamicharts/minio -n minio --create-namespace -f minio-values.yaml
update-minio-cluster:
	  helm upgrade --install minio oci://registry-1.docker.io/bitnamicharts/minio -n minio -f minio-values.yaml
clean-minio-cluster:
	helm delete minio --namespace minio
create-gke-firewall-rules:
	 gcloud compute firewall-rules create fw-rule-gke-node-port --allow tcp:${Port}
