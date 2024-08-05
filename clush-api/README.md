# Clush API

This is the code and configuration used to build and deploy Clush as an API using [AWS Chalice](https://github.com/aws/chalice).

## API Documentation

A rough outline of the API documentation can be found in Clush\_API.md

## Usage

### aws apigateway commands

The examples below use the dev api

#### Dev

The dev api id is sct3w4rt0j

#### Prod

The prod api id is 67rbmn5bu9

#### Determining resource paths of API

Note that the resource IDs retrieved from this command are used in the examples below.
```
 aws --profile dev apigateway get-resources --rest-api-id <API ID>
```

#### Publishing a command to devices

As specificed in the API docs, a **command** is required, but **devices**, **orgs**, **username**, and **password** are not. At least one of **devices** or **orgs** is required to  
generate a device list, and **username** and **password** are helium credentials that are required if generate a device list based on an org if **orgs** is provided.  
Additionally, a **timeout** value can be provided, which is a value in seconds of how long you are willing to wait for a response. Default value is 60. 

```
aws --profile dev apigateway test-invoke-method --rest-api-id adw97ttpj5  --resource-id xx4ad4  --http-method POST  --body '{"command": "whoami.sh",  "devices": ["acu12671.org6512.dev.openpath.local", "acu11375.org6499.dev.openpath.local"], "orgs":[6499], "username":"edward.babbe@motorolasolutions.com", "password":<password here>"}' --headers '{"Content-Type": "application/json"}'
```

#### Retrieving a request result

The only required field is **requestId**. You can also supply **devices**, but **requestId** will also find any devices that reported back that requestId.  
Additionally, a **timeout** value can be provided, which is a value in seconds of how long you are willing to wait for a response. Default value is 60. 
```
aws --profile dev apigateway test-invoke-method --rest-api-id adw97ttpj5  --resource-id 8td1p2  --http-method POST  --body '{"requestId": "641b6ac3-8ed0-40b0-a0fc-f893785ec7d3"}' --headers '{"Content-Type": "application/json"}'
```

### CURL

It is possible to do it with curl commands. I have not gotten it to work yet, but [this](https://github.com/awslabs/aws-sigv4-proxy) is needed to sign your http requests.

### Python

It is possible to do this using python, but I also have not gotten it to work yet. 
