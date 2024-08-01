import boto3
import time
from botocore.exceptions import ClientError

def get_resources_list():
    try:
        client = boto3.client('resource-explorer-2')
        view_arn = "Your view ARN here"
        query_string = '-tag.key:Application -tag.key:Environment -tag.key:map-migrated'
        list_of_resources_ids = []
        paginator = client.get_paginator('search')
        response_iterator = paginator.paginate(
            QueryString=query_string,
            ViewArn=view_arn
        )

        for page in response_iterator:
            resources = page['Resources']
            list_of_resources_ids.extend([ item['Arn'] for item in resources])
        
        return list(set(list_of_resources_ids))
    except Exception as err:
        print("Line 22: error getting resource list > \n ",err,"\n Line 22: END")

def group_resources_by_region(list_of_resource_ids):
    try:
        resource_groups = {}
        for arn in list_of_resource_ids:
            if ':' in arn:
                region = arn.split(':')[3]
                if region not in resource_groups:
                    resource_groups[region] = []
                resource_groups[region].append(arn)
        # resource_groups.pop('') # will exclude global services for now
        print("Group by region: ")
        print(resource_groups.keys())
        return resource_groups
    except Exception as e:
        print("Error in function to group the resources by region {}".format(e))


def tag_kms_resource(arn, region):
    kms_client = boto3.client('kms', region_name=region)
    key_id = arn.split('/')[-1]
    try:
        kms_client.tag_resource(
            KeyId=key_id,
            Tags=[
                {'TagKey': 'Application', 'TagValue': 'EngageSparrow'},
                {'TagKey': 'Environment', 'TagValue': 'staging'},
                {'TagKey': 'map-migrated', 'TagValue': 'migB3CDHXH2IL'}
            ]
        )
        return True
    except Exception as e:
        print(f"Failed to tag KMS resource {arn}: {str(e)}")
        return False


def tag_resources_by_region(resource_groups):
    Not_Tagged_List = []
    Tagged_list = []
    for region, resource_list in resource_groups.items():
        tag_resources_client = boto3.client('resourcegroupstaggingapi', region_name=region if region else "us-east-1")
        batches = [resource_list[i:i + 10] for i in range(0, len(resource_list), 10)]
        
        for batch in batches:
            retry_count = 0
            kms_resources = [arn for arn in batch if ':kms:' in arn]
            non_kms_resources = [arn for arn in batch if ':kms:' not in arn]
            while retry_count < 3:
                try:
                    for kms_arn in kms_resources:
                        if tag_kms_resource(kms_arn, region):
                            Tagged_list.append(kms_arn)
                        else:
                            Not_Tagged_List.append(kms_arn)
                    if non_kms_resources:
                        tag_response = tag_resources_client.tag_resources(
                            ResourceARNList=batch,
                            Tags={
                                'Application': 'EngageSparrow',
                                'Environment': 'staging',
                                'map-migrated': 'migB3CDHXH2IL'
                            }
                        )
                        if tag_response['FailedResourcesMap']:
                            for arn, failure in tag_response['FailedResourcesMap'].items():
                                if failure['ErrorCode'] == 'InvalidArgument':
                                    print(f"Resource type not supported for tagging: {arn}")
                                else:
                                    print(f"Failed to tag {arn}: {failure['ErrorCode']} - {failure['ErrorMessage']}")
                                Not_Tagged_List.append(arn)
                        Tagged_list.extend([arn for arn in batch if arn not in tag_response['FailedResourcesMap']])
                    break  # Success, exit retry loop
                except ClientError as e:
                    if e.response['Error']['Code'] == 'Throttling':
                        retry_count += 1
                        wait_time = 2 ** retry_count  
                        print(f"Throttled. Retrying in {wait_time} seconds...")
                        time.sleep(wait_time)
                    else:
                        print(f"ClientError while tagging batch: {e}")
                        Not_Tagged_List.extend(batch)
                        break
                except Exception as e:
                    print(f"Unexpected error while tagging batch: {e}")
                    Not_Tagged_List.extend(batch)
                    break
            else:
                print(f"Failed to tag batch after 3 retries. Moving to next batch.")
                Not_Tagged_List.extend(batch)
            
            time.sleep(1) 

    print(f"Successfully tagged {len(Tagged_list)} resources")
    print(f"Failed to tag {len(Not_Tagged_List)} resources")
    return Not_Tagged_List, Tagged_list

# def tag_resources_by_region(resource_groups):
#     try:
#         Not_Tagged_List =  []
#         Tagged_list = []
#         validRegion = 0
#         invalidRegion = 0
#         for region, resource_list in resource_groups.items():
#             if region:
#                 validRegion +=1
#                 tag_resources_client = boto3.client('resourcegroupstaggingapi', region_name=region)
#                 batches = [resource_list[i:i + 20] for i in range(0, len(resource_list), 20)]
                
#                 for batch in batches:
#                     try:
#                         tag_response = tag_resources_client.tag_resources(
#                             ResourceARNList=batch,
#                             Tags={'Application' : 'EngageSparrow',
#                             'Environment':'staging',
#                             'map-migrated':'migB3CDHXH2IL'
#                             }
#                         )
#                         print("if: batch in batches> :",batch)
#                         print("tag_response if block: ",tag_response)
#                         if tag_response['FailedResourcesMap']:
#                             Not_Tagged_List.extend(list(tag_response['FailedResourcesMap'].keys()))
#                         elif tag_response:
#                             Tagged_list.extend(batch)
#                     except Exception as e:
#                         print("while tagging a resource: (in if block)",e)
                        
#             else:
#                 invalidRegion += 1
#                 tag_resources_client = boto3.client('resourcegroupstaggingapi', region_name="us-east-1")
#                 batches = [resource_list[i:i + 20] for i in range(0, len(resource_list), 20)]
#                 for batch in batches:
#                     try:
#                         tag_response = tag_resources_client.tag_resources(
#                             ResourceARNList=batch,
#                             Tags={'Application' : 'EngageSparrow',
#                             'Environment':'staging',
#                             'map-migrated':'migB3CDHXH2IL'
#                             }
#                         )
#                         print("else: batch in batches> :",batch)
#                         print("tag_response else block: ",tag_response)
#                         if tag_response['FailedResourcesMap']:
#                             Not_Tagged_List.extend(list(tag_response['FailedResourcesMap'].keys()))
#                         elif tag_response:
#                             Tagged_list.extend(batch)
#                     except Exception as e:
#                         print("while tagging a resource (in else block): ",e)

#         print("Tagged resources List:",Tagged_list)
#         print("valid regions :",validRegion)
#         print("invalid regions :",invalidRegion)
#         return Not_Tagged_List
#     except Exception as e:
#         print("Error in tag resources by region function {}".format(e))

def lambda_handler(event, context):
    try:
        print("starting the execution now..")
        Resourcelist = get_resources_list()
        if Resourcelist:
            print("Invoking the resource grouping by regions value")
            ResourceGroups = group_resources_by_region(Resourcelist)
            print("resourceGroups: >",ResourceGroups)
            if ResourceGroups:
                print("Invoking the tag_resources_by_region")
                Failed_to_tag,tagged = tag_resources_by_region(ResourceGroups)
                print ("Resources failed to tag: ",Failed_to_tag)
                print ("Resources tagged: ",tagged)
    except Exception as e:
        print("Error in face record creation {}".format(e))
