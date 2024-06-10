install_superset:
	helm install superset superset/superset -n superset --create-namespace -f superset-values.yaml

upgrade_superset:
	helm upgrade --install superset superset/superset -n superset -f superset-values.yaml
# trino://admin@my-trinio-trino.trino.svc.cluster.local:8080/delta/silver