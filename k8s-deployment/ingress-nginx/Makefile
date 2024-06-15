.PHONY: ingress-nginx

ingress-nginx:
    helm upgrade --install ingress ingress-nginx/ingress-nginx -n ingress --create-namespace -f ingress-nginx-values.yaml
clean-ingress-nginx:
	helm delete ingress --namespace ingress