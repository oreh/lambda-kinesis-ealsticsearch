if [ -e maraca.zip ]; then
  rm maraca.zip
fi

zip -r maraca.zip index.js node_modules

#--role arn:aws:iam::952842231633:role/iotSG/Maraca-LambdaExecutionRole-10GGGGUXYJ9UM \
aws lambda upload-function \
  --region us-east-1 \
  --function-name k5s2es_maraca \
  --function-zip $PWD/maraca.zip \
  --role arn:aws:iam::952842231633:role/lambda_kinesis_role \
  --mode event \
  --handler index.handler \
  --runtime nodejs
