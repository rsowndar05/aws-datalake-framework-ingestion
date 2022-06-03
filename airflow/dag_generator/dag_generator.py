import boto3

s3_client = boto3.client("s3")
template_bucket = 'dl-fmwrk-code-us-east-2'
airflow_bucket = 'dl-fmwrk-mwaa-us-east-2'

def lambda_handler(event, context):
  source_id=event['source_id']
  asset_id=event['asset_id']
  schedule=event['schedule']
  template_object_key = "airflow-template/dl_fmwrk_dag_template.py"
  dag_id = f"{source_id}_{asset_id}_worflow"
  file_name= f"dags/{source_id}_{asset_id}_worflow.py"
  
  file_content = s3_client.get_object(Bucket=template_bucket, Key=template_object_key)["Body"].read()
  file_content=file_content.decode()
  
  file_content=file_content.replace("src_sys_id_placeholder", source_id)
  file_content=file_content.replace("ast_id_placeholder", asset_id)
  file_content=file_content.replace("dag_id_placeholder", dag_id)
  if schedule == "None":
    file_content=file_content.replace('"schedule_placeholder"', "None")
  else:
    file_content=file_content.replace("schedule_placeholder", schedule)
  
  file = bytes(file_content, encoding='utf-8')
  s3_client.put_object(Bucket=airflow_bucket, Body=file, Key=file_name)
    
  return {
    'statusCode': 200,
    'body': f"Upload succeeded: {file_name} has been uploaded to Amazon S3 in bucket {airflow_bucket}"
    }    
      
  
  
      
