import configparser
import boto3
import json
from botocore.exceptions import ClientError
from time import sleep

# read a config file and load parameters for cluster
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

def create_cluster():
    """
    Creates a Redshift cluster and returns its endpoint.
    
    Return :
    - endpoint : the URL address of the host
    """
    
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
    
    # pass if cluser already exists
    except Exception as e:
        if e.response['Error']['Code'] != 'ClusterAlreadyExists':
            print(e)
    
    # set security group of cluster
    set_security_group(redshift.describe_clusters(ClusterIdentifier=CLUSTER_IDENTIFIER)['Clusters'][0]['VpcId'])
    
    # get endpoint of cluster
    endpoint = get_endpoint()
    
    return endpoint

def get_endpoint():
    """
    Checks the status of cluster and return endpoint if available.
    
    Return :
    - endpoint : the URL address of the host
    """
    try:
        while(redshift.describe_clusters(ClusterIdentifier=CLUSTER_IDENTIFIER)['Clusters'][0]["ClusterStatus"] == "creating"):
            sleep(30)   

        if redshift.describe_clusters(ClusterIdentifier=CLUSTER_IDENTIFIER)['Clusters'][0]["ClusterStatus"] == "available":
            return redshift.describe_clusters(ClusterIdentifier=CLUSTER_IDENTIFIER)['Clusters'][0]['Endpoint']['Address']
        else:
            return None
    except Exception as e:
        return None

def set_security_group(vpc_id):
    """
    Opens an incoming TCP port to access the cluster endpoint.
    
    params :
    - vpc_id : vpc id of cluster
    """
    
    # create client for ec2
    ec2 = boto3.resource('ec2',
                       region_name="us-west-2",
                       aws_access_key_id=KEY,
                       aws_secret_access_key=SECRET)
    
    # open to all non local ip with DB_PORT
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
    # pass if permission already exists
    except Exception as e:
        if e.response['Error']['Code'] != 'InvalidPermission.Duplicate':
            print(e)

def delete_cluster():
    """
    deletes the cluster created above.
    """
    
    try:
        # delete cluster
        redshift.delete_cluster(ClusterIdentifier=CLUSTER_IDENTIFIER, 
                            SkipFinalClusterSnapshot=True)
        # check until cluster is completely deleted 
        while(redshift.describe_clusters(ClusterIdentifier=CLUSTER_IDENTIFIER)['Clusters'][0]["ClusterStatus"] == "deleting"):
            sleep(30)
            
    except Exception as e:
        # ignore an error after deletion
        if e.response['Error']['Code'] != 'ClusterNotFound':
            print(e)
        

