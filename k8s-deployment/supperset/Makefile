.PHONY: spark-cluster
install_superset:
	helm install superset superset/superset -n superset --create-namespace -f superset-values.yaml

upgrade_superset:
	helm upgrade --install superset superset/superset -n superset -f superset-values.yaml
clean:
	kubectl delete ns superset
