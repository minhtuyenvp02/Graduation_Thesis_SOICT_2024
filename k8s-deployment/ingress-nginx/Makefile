.PHONY: ingress-nginx

ingress-nginx:

    helm install ingress oci://ghcr.io/nginxinc/charts/nginx-ingress --version 1.2.1 -n ingress --create-namespace -f ingress-nginx-values.yaml
    # helm repo add confluentinc https://packages.confluent.io/helm
# helm repo update
# helm upgrade --install confluent-operator confluentinc/confluent-for-kubernetes --namespace kafka --create-namespace

clean-ingress-nginx:
	helm delete ingress --namespace ingress