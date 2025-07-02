"""
azure-cspm-benchmark.py

Assists with provisioning calculations by retrieving a count
of all billable resources attached to an Azure subscription.
"""

import csv
import logging
import subprocess
import json

from functools import cached_property, lru_cache
from azure.identity import AzureCliCredential
from azure.mgmt.resource import ResourceManagementClient, SubscriptionClient
from azure.mgmt.containerservice import ContainerServiceClient
from azure.mgmt.compute import ComputeManagementClient
from azure.mgmt.containerinstance import ContainerInstanceManagementClient
from azure.mgmt.appcontainers import ContainerAppsAPIClient
import msrestazure.tools
from tabulate import tabulate

headers = {
    'tenant_id': 'Azure Tenant ID',
    'subscription_id': 'Azure Subscription ID',
    'aks_nodes': 'Kubernetes Nodes',
    'vms': 'Virtual Machines',
    'aci_containers': 'Container Instances',
    'aca_apps': 'Container Apps'
}


class AzureHandle:
    def __init__(self):
        # Acquire a credential object using CLI-based authentication.
        self.creds = AzureCliCredential()

    @cached_property
    def subscription_client(self):
        return SubscriptionClient(self.creds)

    @cached_property
    def subscriptions(self):
        return list(self.subscription_client.subscriptions.list())

    @property
    def tenants(self):
        return list(self.subscription_client.tenants.list())

    def aci_resources(self, subscription_id):
        client = self.resource_client(subscription_id)
        return client.resources.list(filter="resourceType eq 'microsoft.containerinstance/containergroups'")

    def aks_resources(self, subscription_id):
        client = self.resource_client(subscription_id)
        return client.resources.list(filter="resourceType eq 'microsoft.containerservice/managedclusters'")

    def vmss_resources(self, subscription_id):
        client = self.resource_client(subscription_id)
        return client.resources.list(filter="resourceType eq 'Microsoft.Compute/virtualMachineScaleSets'")

    def vms_resources(self, subscription_id):
        client = self.resource_client(subscription_id)
        return client.resources.list(filter="resourceType eq 'Microsoft.Compute/virtualMachines'")

    def aca_resources(self, subscription_id):
        client = self.resource_client(subscription_id)
        return client.resources.list(filter="resourceType eq 'Microsoft.App/containerApps'")

    def managed_clusters(self, subscription_id):
        return self.container_client(subscription_id).managed_clusters.list()

    def rhos_clusters(self, subscription_id):
        return self.container_client(subscription_id).open_shift_managed_clusters.list()

    def container_vmss(self, aks_resource):
        parsed_id = msrestazure.tools.parse_resource_id(aks_resource.id)
        client = self.container_client(parsed_id['subscription'])
        return client.agent_pools.list(resource_group_name=parsed_id['resource_group'],
                                       resource_name=parsed_id['resource_name'])

    def container_aci(self, aci_resource):
        parsed_id = msrestazure.tools.parse_resource_id(aci_resource.id)
        client = self.container_instance_client(parsed_id['subscription'])
        return client.container_groups.get(resource_group_name=parsed_id['resource_group'],
                                           container_group_name=parsed_id['resource_name']).containers

    def vms_inside_vmss(self, vmss_resource):
        parsed_id = msrestazure.tools.parse_resource_id(vmss_resource.id)
        client = ComputeManagementClient(self.creds, parsed_id['subscription'])
        return client.virtual_machine_scale_set_vms.list(resource_group_name=parsed_id['resource_group'],
                                                         virtual_machine_scale_set_name=vmss_resource.name)

    def aca_running_containers(self, aca_resource):
        """Get running container count using Azure CLI as the SDK methods are unreliable"""
        parsed_id = msrestazure.tools.parse_resource_id(aca_resource.id)
        
        try:
            # Use Azure CLI to get container app replica information
            cmd = [
                'az', 'containerapp', 'replica', 'list',
                '--name', parsed_id['resource_name'],
                '--resource-group', parsed_id['resource_group'],
                '--output', 'json'
            ]
            
            result = subprocess.run(cmd, capture_output=True, text=True, check=True)
            replicas_data = json.loads(result.stdout)
            
            running_containers = 0
            
            # Count running containers across all replicas
            for replica in replicas_data:
                properties = replica.get('properties', {})
                if properties.get('runningState') == 'Running':
                    # Each replica typically has 1 container, but let's check containers array
                    containers = properties.get('containers', [])
                    if containers:
                        # Count running containers in this replica
                        running_in_replica = sum(1 for c in containers if c.get('runningState') == 'Running')
                        running_containers += running_in_replica
                    else:
                        # If no containers array, assume 1 container per running replica
                        running_containers += 1
            
            log.debug("Container App %s has %d running containers via CLI", 
                     aca_resource.name, running_containers)
            
            return running_containers
            
        except subprocess.CalledProcessError as e:
            log.debug("CLI command failed for %s: %s", aca_resource.name, str(e))
            
            # Fallback to SDK approach if CLI fails
            try:
                return self.aca_running_containers_sdk(aca_resource)
            except:
                return 0
                
        except Exception as e:
            log.debug("CLI approach failed for %s: %s", aca_resource.name, str(e))
            return 0

    def aca_running_containers_sdk(self, aca_resource):
        """Original SDK-based approach as fallback"""
        parsed_id = msrestazure.tools.parse_resource_id(aca_resource.id)
        client = self.container_apps_client(parsed_id['subscription'])
        
        try:
            # Get the container app details
            app = client.container_apps.get(
                resource_group_name=parsed_id['resource_group'],
                container_app_name=parsed_id['resource_name']
            )
            
            running_count = 0
            
            # Get app properties
            if hasattr(app, 'properties') and app.properties:
                # Check provisioning state
                provisioning_state = getattr(app.properties, 'provisioning_state', '')
                
                if provisioning_state == 'Succeeded':
                    # Get latest revision name
                    latest_revision_name = getattr(app.properties, 'latest_revision_name', None)
                    
                    if latest_revision_name:
                        try:
                            # Get revision details
                            revision = client.container_apps_revisions.get_revision(
                                resource_group_name=parsed_id['resource_group'],
                                container_app_name=parsed_id['resource_name'],
                                revision_name=latest_revision_name
                            )
                            
                            if hasattr(revision, 'properties') and revision.properties:
                                # Get replica count from revision properties
                                replicas = getattr(revision.properties, 'replicas', 0)
                                
                                # If replicas is 0, try to get from template scale settings
                                if replicas == 0:
                                    template = getattr(revision.properties, 'template', None)
                                    if template:
                                        scale = getattr(template, 'scale', None)
                                        if scale:
                                            replicas = max(
                                                getattr(scale, 'min_replicas', 1),
                                                1
                                            )
                                
                                # Get container count from template
                                template = getattr(revision.properties, 'template', None)
                                if template:
                                    containers = getattr(template, 'containers', [])
                                    container_count = len(containers) if containers else 1
                                    running_count = replicas * container_count
                                    
                                    log.debug("Container App %s: %d replicas × %d containers = %d total", 
                                             aca_resource.name, replicas, container_count, running_count)
                                
                        except Exception as revision_error:
                            log.debug("Could not get revision details for %s: %s", aca_resource.name, str(revision_error))
                            
                            # Final fallback: assume 1 container if app is provisioned successfully
                            running_count = 1
            
            return running_count
            
        except Exception as e:
            log.warning("Unable to get details for Container App %s: %s", aca_resource.name, str(e))
            return 0

    @lru_cache
    def container_client(self, subscription_id):
        return ContainerServiceClient(self.creds, subscription_id)

    @lru_cache
    def container_instance_client(self, subscription_id):
        return ContainerInstanceManagementClient(self.creds, subscription_id)

    @lru_cache
    def container_apps_client(self, subscription_id):
        return ContainerAppsAPIClient(self.creds, subscription_id)

    @lru_cache
    def resource_client(self, subscription_id):
        return ResourceManagementClient(self.creds, subscription_id)


LOG_LEVEL = logging.INFO
LOG_LEVEL = logging.DEBUG
log = logging.getLogger('azure')
log.setLevel(LOG_LEVEL)
ch = logging.StreamHandler()
ch.setLevel(LOG_LEVEL)
formatter = logging.Formatter('%(asctime)s %(levelname)-8s %(message)s', '%Y-%m-%d %H:%M:%S')
ch.setFormatter(formatter)
log.addHandler(ch)

for mod in ['azure.identity._internal.decorators', 'azure.core.pipeline.policies.http_logging_policy']:
    logging.getLogger(mod).setLevel(logging.WARNING)


data = []
totals = {'tenant_id': 'totals', 'subscription_id': 'totals', 'aks_nodes': 0, 'vms': 0, 'aci_containers': 0, 'aca_apps': 0}
az = AzureHandle()

log.info("You have access to %d subscription(s) within %s tenant(s)", len(az.subscriptions), len(az.tenants))
for subscription in az.subscriptions:
    row = {'tenant_id': subscription.tenant_id, 'subscription_id': subscription.subscription_id,
           'aks_nodes': 0, 'vms': 0, 'aci_containers': 0, 'aca_apps': 0}
    log.info("Processing Azure subscription: %s (id=%s)", subscription.display_name, subscription.subscription_id)

    vmss_list = list(az.vmss_resources(subscription.subscription_id))

    # (1) Process AKS
    for aks in az.aks_resources(subscription.subscription_id):
        for node_pool in az.container_vmss(aks):
            log.info("Identified node pool: '%s' within AKS: '%s' with %d node(s)",
                     node_pool.name, aks.name, node_pool.count)
            row['aks_nodes'] += node_pool.count

    # (2) Process VMSS
    for vmss in az.vmss_resources(subscription.subscription_id):
        if vmss.tags is not None and 'aks-managed-createOperationID' in vmss.tags:
            # AKS resources already accounted for above
            continue

        vm_count = sum(1 for vm in az.vms_inside_vmss(vmss))
        log.info("Identified %d vm resource(s) inside Scale Set: '%s'", vm_count, vmss.name)
        row['vms'] += vm_count

    # # (3) Process ACI
    for aci in az.aci_resources(subscription.subscription_id):
        container_count = sum(1 for container in az.container_aci(aci))
        log.info("Identified %d container resource(s) inside Container Group: '%s'", container_count, aci.name)
        row['aci_containers'] += container_count

    # (4) Process VMs
    vm_count = sum((1 for vm in az.vms_resources(subscription.subscription_id)))
    log.info('Identified %d vm resource(s) outside of Scale Sets', vm_count)
    row['vms'] += vm_count

    # (5) Process Azure Container Apps (running containers)
    aca_running_containers = 0
    for aca in az.aca_resources(subscription.subscription_id):
        running_containers = az.aca_running_containers(aca)
        aca_running_containers += running_containers
        log.info("Container App '%s' has %d running container(s)", aca.name, running_containers)
    
    log.info('Total running containers in Azure Container Apps: %d', aca_running_containers)
    row['aca_apps'] = aca_running_containers

    data.append(row)

    totals['vms'] += row['vms']
    totals['aks_nodes'] += row['aks_nodes']
    totals['aci_containers'] += row['aci_containers']
    totals['aca_apps'] += row['aca_apps']

data.append(totals)

# Output our results
print(tabulate(data, headers=headers, tablefmt="grid"))

with open('az-benchmark.csv', 'w', newline='', encoding='utf-8') as csv_file:
    csv_writer = csv.DictWriter(csv_file, fieldnames=headers.keys())
    csv_writer.writeheader()
    csv_writer.writerows(data)

log.info("CSV summary has been exported to ./az-benchmark.csv file")