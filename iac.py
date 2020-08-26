import configparser
import boto3
import json
from botocore.exceptions import ClientError
from asyncio import sleep

config = configparser.ConfigParser()
config.read('dwh.cfg')

KEY = config.get('ACCESS', 'KEY')
SECRET = config.get('ACCESS', 'SECRET')
ROLE_NAME = config.get('IAM_ROLE', 'NAME')

def create_iam_role():
    # create client for IAM
    iam = boto3.client('iam',
        region_name = "us-west-2",
        aws_access_key_id = KEY,
        aws_secret_access_key = SECRET)
    

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
        
    # return the IAM role ARN
    return arn
    


def create_cluster():

    ARN = create_iam_role()
    DWH_CLUSTER_TYPE = config.get('DWH', 'DWH_CLUSTER_TYPE')
    DWH_NUM_NODES = config.get('DWH', 'DWH_NUM_NODES')
    DWH_NODE_TYPE = config.get('DWH', 'DWH_NODE_TYPE')
    CLUSTER_IDENTIFIER = config.get('DWH', 'DWH_CLUSTER_IDENTIFIER')
    DB_NAME = config.get('CLUSTER', 'DB_NAME')
    DB_USER = config.get('CLUSTER', 'DB_USER')
    DB_PW = config.get('CLUSTER', 'DB_PASSWORD')
    
    # create client for Redshift
    redshift = boto3.client('redshift',
        region_name = "us-west-2",
        aws_access_key_id = KEY,
        aws_secret_access_key = SECRET)
    
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
            
            IamRoles = [ARN]
        )
        
    except Exception as e:
        if e.response['Error']['Code'] == 'ClusterAlreadyExists':
            print(CLUSTER_IDENTIFIER + " already exists!")
                
        else:
            print(e)
    else:
        print("Created cluster!")
        
    cluster_props = redshift.describe_clusters(ClusterIdentifier=CLUSTER_IDENTIFIER)['Clusters'][0]
    set_security_group(cluster_props['VpcId'])
    endpoint = get_endpoint(cluster_props)
    
    return endpoint

def get_endpoint(cluster_props):
    while(cluster_props["ClusterStatus"] == "modifying"):
        sleep(10)
        
    if cluster_props["ClusterStatus"] == "available":
        return cluster_props['Endpoint']['Address']
    else:
        return None
        

def set_security_group(vpc_id):
    DB_PORT = config.get('CLUSTER', 'DB_PORT')
    
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
            print("Permission already exists!")
        else:
            print(e)

def delete_cluster():
    pass


if __name__ == "__main__":
    print(create_cluster())