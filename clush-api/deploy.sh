#!/bin/bash
# Check if an environment variable is passed and validate it
if [[ $# -ne 1 ]]; then
    echo "Error: You must provide an environment variable as a command-line argument."
    exit 1
fi
ENVIRONMENT=$1
if [[ "$ENVIRONMENT" != "dev" && "$ENVIRONMENT" != "sandbox" && "$ENVIRONMENT" != "prod" ]]; then
    echo "Error: Environment variable must be 'dev', 'sandbox', or 'prod'."
    exit 1
fi
# Check if logged into AWS CLI for the specified environment
if ! aws sts get-caller-identity --profile "$ENVIRONMENT" > /dev/null 2>&1; then
    echo "Error: Not logged into awscli for the environment $ENVIRONMENT."
    exit 1
fi
# Run the aws route53 list-hosted-zones command
HOSTED_ZONE_JSON=$(aws route53 list-hosted-zones --query "HostedZones[?Name==\`${ENVIRONMENT}.alta.avigilon.com.\`]" --profile "$ENVIRONMENT")
if [ "$HOSTED_ZONE_JSON" == "[]" ]; then
    echo "Error: No hosted zones found for $ENVIRONMENT.alta.avigilon.com."
    exit 1
fi
# Extract the zone ID
ZONE_ID_FULL=$(echo "$HOSTED_ZONE_JSON" | jq -r '.[0].Id')
ZONE_ID=`echo $ZONE_ID_FULL | cut -d/ -f3- ` # Remove everything before and including the first "/"
# Check for the existence of an AWS ACM certificate
CERTIFICATE_JSON=$(aws acm list-certificates --query "CertificateSummaryList[?DomainName==\`clush-api.${ENVIRONMENT}.alta.avigilon.com\`]" --profile "$ENVIRONMENT")
CERTIFICATE_ARN=$(echo "$CERTIFICATE_JSON" | jq -r '.[0].CertificateArn')
if [ ! ${#CERTIFICATE_ARN} -gt 15 ]; then
    # Request a new certificate if it does not exist
    REQUEST_CERT_JSON=$(aws acm request-certificate \
        --domain-name "clush-api.${ENVIRONMENT}.alta.avigilon.com" \
        --validation-method DNS \
        --idempotency-token 12345 \
        --options CertificateTransparencyLoggingPreference=DISABLED \
        --profile "$ENVIRONMENT")
    CERTIFICATE_ARN=$(echo "$REQUEST_CERT_JSON" | jq -r '.CertificateArn')
    sleep 15
else
    echo "Certificate already exists: $CERTIFICATE_ARN"
fi
# Describe the certificate to get the DNS validation CNAME details
VALIDATION_JSON=$(aws acm describe-certificate --certificate-arn "$CERTIFICATE_ARN" --query "Certificate.DomainValidationOptions[0]" --profile "$ENVIRONMENT")
RECORD_NAME=$(echo "$VALIDATION_JSON" | jq -r '.ResourceRecord.Name')
RECORD_VALUE=$(echo "$VALIDATION_JSON" | jq -r '.ResourceRecord.Value')
# Create a CNAME record in Route 53 for domain validation
CNAME_RESULT=$(aws route53 list-resource-record-sets --hosted-zone-id "$ZONE_ID"  --query "ResourceRecordSets[?Name==\`${RECORD_NAME}\`]" --profile "$ENVIRONMENT")
if [ ! ${#CNAME_RESULT} -gt 15 ]; then
    CHANGE_BATCH=$(cat <<-EOF
        {
          "Changes": [
            {
              "Action": "CREATE",
              "ResourceRecordSet": {
                "Name": "$RECORD_NAME",
                "Type": "CNAME",
                "TTL": 300,
                "ResourceRecords": [{"Value": "$RECORD_VALUE"}]
              }
            }
          ]
        })
    #echo $CHANGE_BATCH
    aws route53 change-resource-record-sets --hosted-zone-id "$ZONE_ID" --change-batch "$CHANGE_BATCH" --profile "$ENVIRONMENT"
fi
# Wait for the certificate to be validated
aws acm wait certificate-validated --certificate-arn "$CERTIFICATE_ARN" --profile "$ENVIRONMENT"

# Check for the existence of .chalice/config.json
CHALICE_CONFIG=".chalice/config.json"
if [ ! -f "$CHALICE_CONFIG" ]; then
    echo "Error: $CHALICE_CONFIG does not exist."
    exit 1
fi

# Update the .chalice/config.json file with the domain name and certificate ARN
jq --arg env "$ENVIRONMENT" --arg domain "clush-api.${ENVIRONMENT}.alta.avigilon.com" --arg arn "$CERTIFICATE_ARN" \
    '.stages[$env].api_gateway_custom_domain.domain_name=$domain | .stages[$env].api_gateway_custom_domain.certificate_arn=$arn' \
    "$CHALICE_CONFIG" > tmp.$$.json && mv tmp.$$.json "$CHALICE_CONFIG"

exec 5>&1
# Deploy with Chalice
DEPLOY_OUTPUT=$(chalice deploy --profile "$ENVIRONMENT" --stage "$ENVIRONMENT"  | tee /dev/fd/5 )
if [ $? -ne 0 ]; then
    echo "Error: Chalice deploy failed."
    exit 1
fi

# Extract hosted zone ID and alias domain name from Chalice deploy output
DEPLOYED_ZONE_ID=$(echo "$DEPLOY_OUTPUT" | grep 'HostedZoneId:' | awk '{print $2}')
ALIAS_DOMAIN_NAME=$(echo "$DEPLOY_OUTPUT" | grep 'AliasDomainName:' | awk '{print $2}')
A_RESULT=$(aws route53 list-resource-record-sets --hosted-zone-id "$ZONE_ID"  --query "ResourceRecordSets[?Name==\`clush-api.${ENVIRONMENT}.alta.avigilon.com.\`]" --profile "$ENVIRONMENT")
if [ ! ${#A_RESULT} -gt 15 ]; then
    # Create a Route 53 record
    CHANGE_BATCH=$(cat <<-EOF
    {
      "Changes": [
        {
          "Action": "CREATE",
          "ResourceRecordSet": {
            "Name": "clush-api.${ENVIRONMENT}.alta.avigilon.com",
            "Type": "A",
            "AliasTarget": {
              "DNSName": "$ALIAS_DOMAIN_NAME",
              "HostedZoneId": "$DEPLOYED_ZONE_ID",
              "EvaluateTargetHealth": false
            }
          }
        }
      ]
    })
    aws route53 change-resource-record-sets --hosted-zone-id "$ZONE_ID" --change-batch "$CHANGE_BATCH" --profile "$ENVIRONMENT"
fi

if [ $? -eq 0 ]; then
    echo "Success: All steps have completed successfully."
else
    echo "Error: Failed to create Route 53 record."
    exit 1
fi
