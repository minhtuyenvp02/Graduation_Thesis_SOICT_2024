#!/bin/bash
nodeports=(30092 30093 30094)

# Loop through each NodePort and create firewall rules
for port in "${nodeports[@]}"
do
    gcloud compute firewall-rules create allow-nodeport-${port} \
        --allow=tcp:${port} \
        --network=default \
        --direction=INGRESS \
        --source-ranges=0.0.0.0/0 \
        --target-tags=k8s-node
done