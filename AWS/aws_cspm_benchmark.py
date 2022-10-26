"""
aws-cspm-benchmark.py

Assists with provisioning calculations by retrieving a count of
all billable resources attached to an AWS account.

Author: Joshua Hiller @ CrowdStrike
Creation date: 03.23.21
"""
import csv
from functools import cached_property
import boto3
from tabulate import tabulate


data = []
headers = {
            "region": "Region",
            "ecs": "ECS - Clusters",
            "vms_terminated": "Terminated VMs",
            "vms_running": "Running VMs",
            'kubenodes_terminated': "Terminated Kubernetes Nodes",
            'kubenodes_running': "Running Kubernetes Nodes"
}
totals = {
            "region": "TOTAL",
            "ecs": 0,
            "vms_terminated": 0,
            "vms_running": 0,
            'kubenodes_terminated': 0,
            'kubenodes_running': 0
}


class AWSOrgAccess:
    def accounts(self):
        try:
            client = boto3.client('organizations')
            response = client.list_accounts()
            accounts = response['Accounts']
            next_token = response.get('NextToken', None)

            while next_token:
                response = client.list_accounts(NextToken=next_token)
                accounts += response['Accounts']
                next_token = response.get('NextToken', None)

            return [self.aws_handle(a) for a in accounts]
        except client.exceptions.AccessDeniedException:
            print("Cannot autodiscover adjacent accounts: cannot list accounts within the AWS organization")
            return [AWSHandle()]

    def aws_handle(self, account):
        if account['Id'] == self.master_account_id:
            return AWSHandle(aws_session=self.master_session)
        return AWSHandle(aws_session=self.new_session(account['Id']))

    def new_session(self, account_id):
        try:
            credentials = self.master_sts.assume_role(
                RoleArn=f'arn:aws:iam::{account_id}:role/OrganizationAccountAccessRole',
                RoleSessionName=account_id
            )
            return boto3.session.Session(
                aws_access_key_id=credentials['Credentials']['AccessKeyId'],
                aws_secret_access_key=credentials['Credentials']['SecretAccessKey'],
                aws_session_token=credentials['Credentials']['SessionToken'],
                region_name='us-east-1'
               )
        except self.master_sts.exceptions.ClientError as exc:
            print("Cannot access adjacent account: ", account_id, exc)
            raise exc

    @cached_property
    def master_session(self):
        return boto3.session.Session()

    @cached_property
    def master_sts(self):
        return self.master_session.client('sts')

    @cached_property
    def master_account_id(self):
        return self.master_sts.get_caller_identity()["Account"]


class AWSHandle:
    EKS_TAGS = ['eks:cluster-name', 'alpha.eksctl.io/nodegroup-type', 'aws:eks:cluster-name', 'eks:nodegroup-name']

    def __init__(self, aws_session=None):
        self.aws_session = aws_session if aws_session else boto3.session.Session()

    @property
    def regions(self):
        return self.ec2.describe_regions()['Regions']

    def ec2_instances(self, aws_region):
        client = self.aws_session.client('ec2', aws_region)

        response = client.describe_instances(MaxResults=1000)
        instances = response['Reservations']
        next_token = response['NextToken'] if 'NextToken' in response else None

        while next_token:
            response = client.describe_instances(MaxResults=1000, NextToken=next_token)
            instances += response['Reservations']
            next_token = response['NextToken'] if 'NextToken' in response else None

        return instances

    @cached_property
    def ec2(self):
        return self.aws_session.client("ec2")

    @classmethod
    def is_vm_kubenode(cls, vm):
        return any(True for tag in vm.get('Tags', []) if tag['Key'] in cls.EKS_TAGS)

    @classmethod
    def is_vm_running(cls, vm):
        return vm['State']['Name'] != 'stopped'


for aws in AWSOrgAccess().accounts():
    for region in aws.regions:
        RegionName = region["RegionName"]

        # Setup the branch
        print(f"Processing {RegionName}")
        # Create the row for our output table
        row = {'region': RegionName, 'vms_terminated': 0, 'vms_running': 0,
               'kubenodes_terminated': 0, 'kubenodes_running': 0}

        # Count ec2 instances
        for reservation in aws.ec2_instances(RegionName):
            for instance in reservation['Instances']:
                typ = 'kubenode' if AWSHandle.is_vm_kubenode(instance) else 'vm'
                state = 'running' if AWSHandle.is_vm_running(instance) else 'terminated'
                key = f"{typ}s_{state}"
                row[key] += 1

        for k in ['vms_terminated', 'vms_running', 'kubenodes_terminated', 'kubenodes_running']:
            totals[k] += row[k]

        # Add the row to our display table
        data.append(row)
    # Add in our grand totals to the display table
data.append(totals)

# Output our results
print(tabulate(data, headers=headers, tablefmt="grid"))

with open('benchmark.csv', 'w', newline='', encoding='utf-8') as csv_file:
    csv_writer = csv.DictWriter(csv_file, fieldnames=headers.keys())
    csv_writer.writeheader()
    csv_writer.writerows(data)

print("\nCSV file stored in: ./benchmark.csv\n\n")


#     .wwwwwwww.
#   .w"  "WW"  "w.
#  ."   /\  /\   ".
# |\     o  o     /|
#  \|  ___\/___  |/
#  / \ \_v__v_/ / \
# / | \________/ | \
# >  \   WWWW   /  <
# \   \   ""   /   /
#  \   \      /   /
#  The Count says...
#
#  That's ONE server, TWO servers  ... AH AH AH!
