import configparser
import boto3
import json
from botocore.exceptions import ClientError
from time import sleep

config = configparser.ConfigParser()
config.read('dwh.cfg')

KEY = config.get('ACCESS', 'KEY')
SECRET = config.get('ACCESS', 'SECRET')
IAM_ROLE = config.get('IAM_ROLE', 'ARN')

DWH_CLUSTER_TYPE = config.get('DWH', 'DWH_CLUSTER_TYPE')
DWH_NUM_NODES = config.get('DWH', 'DWH_NUM_NODES')
DWH_NODE_TYPE = config.get('DWH', 'DWH_NODE_TYPE')
CLUSTER_IDENTIFIER = config.get('DWH', 'DWH_CLUSTER_IDENTIFIER')
DB_NAME = config.get('CLUSTER', 'DB_NAME')
DB_USER = config.get('CLUSTER', 'DB_USER')
DB_PW = config.get('CLUSTER', 'DB_PASSWORD')
DB_PORT = config.get('CLUSTER', 'DB_PORT')

# create client for Redshift
redshift = boto3.client('redshift',
                        region_name = "us-west-2",
                        aws_access_key_id = KEY,
                        aws_secret_access_key = SECRET)
"""
# create client for IAM
iam = boto3.client('iam',
                   region_name = "us-west-2",
                   aws_access_key_id = KEY,
                   aws_secret_access_key = SECRET)

def create_iam_role():
    
    try:
        # create the IAM role
        iamRole = iam.create_role(
            Path='/',
            RoleName = ROLE_NAME,
            AssumeRolePolicyDocument=json.dumps(
                {
                    'Statement': [{'Action': 'sts:AssumeRole',
                                   'Effect': 'Allow',
                                   'Principal': {'Service': 'redshift.amazonaws.com'}}],
                    'Version': '2012-10-17'
                }))
    except iam.exceptions.EntityAlreadyExistsException:
        # if the IAM role exists, return its ARN
        print("IAM role already exists!")
        arn = iam.get_role(RoleName=ROLE_NAME)['Role']['Arn']
    except ... as e:
        print(e)
        arn = None
    else:
        print("Created IAM role!")

        # attach policy
        iam.attach_role_policy(RoleName=ROLE_NAME,
        PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadONlyAccess")['ResponseMetadata']['HTTPStatusCode']
        
        arn = iam.get_role(RoleName=ROLE_NAME)['Role']['Arn']
    
    config.set('IAM_ROLE', 'ARN', arn)
    # return the IAM role ARN
    return arn
    
"""

def create_cluster():
    try:
        # create cluster
        response = redshift.create_cluster(
            ClusterType = DWH_CLUSTER_TYPE,
            NodeType = DWH_NODE_TYPE,
            NumberOfNodes = int(DWH_NUM_NODES),
            
            DBName = DB_NAME,
            ClusterIdentifier = CLUSTER_IDENTIFIER,
            MasterUsername = DB_USER,
            MasterUserPassword = DB_PW,
            
            IamRoles = [IAM_ROLE]
        )
        
    except Exception as e:
        if e.response['Error']['Code'] == 'ClusterAlreadyExists':
            print(CLUSTER_IDENTIFIER + " already exists!")
                
        else:
            print(e)
    else:
        print("Created cluster!")
        
    set_security_group(redshift.describe_clusters(ClusterIdentifier=CLUSTER_IDENTIFIER)['Clusters'][0]['VpcId'])
    endpoint = get_endpoint()
    
    return endpoint

def get_endpoint():
    try:
        while(redshift.describe_clusters(ClusterIdentifier=CLUSTER_IDENTIFIER)['Clusters'][0]["ClusterStatus"] == "creating"):
            print("creating")
            sleep(30)   

        if redshift.describe_clusters(ClusterIdentifier=CLUSTER_IDENTIFIER)['Clusters'][0]["ClusterStatus"] == "available":
            return redshift.describe_clusters(ClusterIdentifier=CLUSTER_IDENTIFIER)['Clusters'][0]['Endpoint']['Address']
        else:
            return None
    except Exception as e:
        return None

def set_security_group(vpc_id):
    
    # create client for ec2
    ec2 = boto3.resource('ec2',
                       region_name="us-west-2",
                       aws_access_key_id=KEY,
                       aws_secret_access_key=SECRET)
    try:
        vpc = ec2.Vpc(id = vpc_id)
        defaultSg = list(vpc.security_groups.all())[0]
        
        defaultSg.authorize_ingress(
            GroupName=defaultSg.group_name , 
            CidrIp='0.0.0.0/0', 
            IpProtocol='TCP', 
            FromPort=int(DB_PORT),
            ToPort=int(DB_PORT)
        )
    except Exception as e:
        if e.response['Error']['Code'] == 'InvalidPermission.Duplicate':
            pass
        else:
            print(e)

def delete_cluster():
    redshift.delete_cluster(ClusterIdentifier=CLUSTER_IDENTIFIER, 
                            SkipFinalClusterSnapshot=True)
    
    try:
        while(redshift.describe_clusters(ClusterIdentifier=CLUSTER_IDENTIFIER)['Clusters'][0]["ClusterStatus"] == "deleting"):
            print("deleting")
            sleep(30)
    except Exception as e:
        if e.response['Error']['Code'] == 'ClusterNotFoundFault':
            print("Deleted cluster!")

