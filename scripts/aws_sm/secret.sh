# E.g. copy_to_stage_and_prod "create" "code_location" "chain"
copy_to_stage_and_prod() {
  COMMAND=$1
  CODE_LOCATION=$2
  BLOCKCHAIN=$3

  DEV_SECRET_NAME=data-platform/dev/$CODE_LOCATION/$BLOCKCHAIN/secret
  STAGE_SECRET_NAME=data-platform/stage/$CODE_LOCATION/$BLOCKCHAIN/secret
  PROD_SECRET_NAME=data-platform/prod/$CODE_LOCATION/$BLOCKCHAIN/secret

  VALUE=$(aws secretsmanager get-secret-value --secret-id $DEV_SECRET_NAME --region us-west-2  | jq -r '.SecretString')

  echo $DEV_SECRET_NAME
  echo $VALUE

  if [ $COMMAND = "update" ]; then
    aws secretsmanager update-secret \
      --secret-id $STAGE_SECRET_NAME \
      --region us-west-2  \
      --secret-string "$VALUE"

    aws secretsmanager update-secret \
      --secret-id $PROD_SECRET_NAME \
      --region us-west-2  \
      --secret-string "$VALUE"
  elif [ $COMMAND = "create" ]; then
    aws secretsmanager create-secret \
      --name $STAGE_SECRET_NAME \
      --region us-west-2  \
      --secret-string "$VALUE"

    aws secretsmanager create-secret \
      --name $PROD_SECRET_NAME \
      --region us-west-2  \
      --secret-string "$VALUE"
  else
    echo "Invalid command. Please use 'update' or 'create' as the COMMAND parameter."
  fi
}

