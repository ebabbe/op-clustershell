---
title: clush-api v1.0.0
language_tabs:
  - shell: Shell
  - http: HTTP
  - javascript: JavaScript
  - ruby: Ruby
  - python: Python
  - php: PHP
  - java: Java
  - go: Go
toc_footers: []
includes: []
search: true
highlight_theme: darkula
headingLevel: 2

---

<!-- Generator: Widdershins v4.0.1 -->

<h1 id="clush-api">clush-api v1.0.0</h1>

> Scroll down for code samples, example requests and responses. Select a language for code samples from the tabs above or the mobile navigation menu.

<h1 id="clush-api-default">Default</h1>

## post__publish

> Code samples

```shell
# You can also use wget
curl -X POST /publish \
  -H 'Content-Type: application/json' \
  -H 'Accept: application/json'

```

```http
POST /publish HTTP/1.1

Content-Type: application/json
Accept: application/json

```

```javascript
const inputBody = '{
  "devices": [],
  "command": "string",
  "orgs": [],
  "timeout": 60,
  "username": null,
  "password": null,
  "namespaceId": 1000
}';
const headers = {
  'Content-Type':'application/json',
  'Accept':'application/json'
};

fetch('/publish',
{
  method: 'POST',
  body: inputBody,
  headers: headers
})
.then(function(res) {
    return res.json();
}).then(function(body) {
    console.log(body);
});

```

```ruby
require 'rest-client'
require 'json'

headers = {
  'Content-Type' => 'application/json',
  'Accept' => 'application/json'
}

result = RestClient.post '/publish',
  params: {
  }, headers: headers

p JSON.parse(result)

```

```python
import requests
headers = {
  'Content-Type': 'application/json',
  'Accept': 'application/json'
}

r = requests.post('/publish', headers = headers)

print(r.json())

```

```php
<?php

require 'vendor/autoload.php';

$headers = array(
    'Content-Type' => 'application/json',
    'Accept' => 'application/json',
);

$client = new \GuzzleHttp\Client();

// Define array of request body.
$request_body = array();

try {
    $response = $client->request('POST','/publish', array(
        'headers' => $headers,
        'json' => $request_body,
       )
    );
    print_r($response->getBody()->getContents());
 }
 catch (\GuzzleHttp\Exception\BadResponseException $e) {
    // handle exception or api errors.
    print_r($e->getMessage());
 }

 // ...

```

```java
URL obj = new URL("/publish");
HttpURLConnection con = (HttpURLConnection) obj.openConnection();
con.setRequestMethod("POST");
int responseCode = con.getResponseCode();
BufferedReader in = new BufferedReader(
    new InputStreamReader(con.getInputStream()));
String inputLine;
StringBuffer response = new StringBuffer();
while ((inputLine = in.readLine()) != null) {
    response.append(inputLine);
}
in.close();
System.out.println(response.toString());

```

```go
package main

import (
       "bytes"
       "net/http"
)

func main() {

    headers := map[string][]string{
        "Content-Type": []string{"application/json"},
        "Accept": []string{"application/json"},
    }

    data := bytes.NewBuffer([]byte{jsonReq})
    req, err := http.NewRequest("POST", "/publish", data)
    req.Header = headers

    client := &http.Client{}
    resp, err := client.Do(req)
    // ...
}

```

`POST /publish`

> Body parameter

```json
{
  "devices": [],
  "command": "string",
  "orgs": [],
  "timeout": 60,
  "username": null,
  "password": null,
  "namespaceId": 1000
}
```

<h3 id="post__publish-parameters">Parameters</h3>

|Name|In|Type|Required|Description|
|---|---|---|---|---|
|body|body|[PublishRequest](#schemapublishrequest)|false|none|

> Example responses

> 200 Response

```json
{
  "results": {},
  "error": null,
  "message": null
}
```

<h3 id="post__publish-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|200|[OK](https://tools.ietf.org/html/rfc7231#section-6.3.1)|Success|[RequestResponse](#schemarequestresponse)|

<aside class="success">
This operation does not require authentication
</aside>

## post__results

> Code samples

```shell
# You can also use wget
curl -X POST /results \
  -H 'Content-Type: application/json' \
  -H 'Accept: application/json'

```

```http
POST /results HTTP/1.1

Content-Type: application/json
Accept: application/json

```

```javascript
const inputBody = '{
  "devices": [],
  "requestId": "string",
  "timeout": 60
}';
const headers = {
  'Content-Type':'application/json',
  'Accept':'application/json'
};

fetch('/results',
{
  method: 'POST',
  body: inputBody,
  headers: headers
})
.then(function(res) {
    return res.json();
}).then(function(body) {
    console.log(body);
});

```

```ruby
require 'rest-client'
require 'json'

headers = {
  'Content-Type' => 'application/json',
  'Accept' => 'application/json'
}

result = RestClient.post '/results',
  params: {
  }, headers: headers

p JSON.parse(result)

```

```python
import requests
headers = {
  'Content-Type': 'application/json',
  'Accept': 'application/json'
}

r = requests.post('/results', headers = headers)

print(r.json())

```

```php
<?php

require 'vendor/autoload.php';

$headers = array(
    'Content-Type' => 'application/json',
    'Accept' => 'application/json',
);

$client = new \GuzzleHttp\Client();

// Define array of request body.
$request_body = array();

try {
    $response = $client->request('POST','/results', array(
        'headers' => $headers,
        'json' => $request_body,
       )
    );
    print_r($response->getBody()->getContents());
 }
 catch (\GuzzleHttp\Exception\BadResponseException $e) {
    // handle exception or api errors.
    print_r($e->getMessage());
 }

 // ...

```

```java
URL obj = new URL("/results");
HttpURLConnection con = (HttpURLConnection) obj.openConnection();
con.setRequestMethod("POST");
int responseCode = con.getResponseCode();
BufferedReader in = new BufferedReader(
    new InputStreamReader(con.getInputStream()));
String inputLine;
StringBuffer response = new StringBuffer();
while ((inputLine = in.readLine()) != null) {
    response.append(inputLine);
}
in.close();
System.out.println(response.toString());

```

```go
package main

import (
       "bytes"
       "net/http"
)

func main() {

    headers := map[string][]string{
        "Content-Type": []string{"application/json"},
        "Accept": []string{"application/json"},
    }

    data := bytes.NewBuffer([]byte{jsonReq})
    req, err := http.NewRequest("POST", "/results", data)
    req.Header = headers

    client := &http.Client{}
    resp, err := client.Do(req)
    // ...
}

```

`POST /results`

> Body parameter

```json
{
  "devices": [],
  "requestId": "string",
  "timeout": 60
}
```

<h3 id="post__results-parameters">Parameters</h3>

|Name|In|Type|Required|Description|
|---|---|---|---|---|
|body|body|[ResultsRequest](#schemaresultsrequest)|false|none|

> Example responses

> 200 Response

```json
{
  "results": {},
  "error": null,
  "message": null
}
```

<h3 id="post__results-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|200|[OK](https://tools.ietf.org/html/rfc7231#section-6.3.1)|Success|[RequestResponse](#schemarequestresponse)|

<aside class="success">
This operation does not require authentication
</aside>

# Schemas

<h2 id="tocS_PublishRequest">PublishRequest</h2>
<!-- backwards compatibility -->
<a id="schemapublishrequest"></a>
<a id="schema_PublishRequest"></a>
<a id="tocSpublishrequest"></a>
<a id="tocspublishrequest"></a>

```json
{
  "devices": [],
  "command": "string",
  "orgs": [],
  "timeout": 60,
  "username": null,
  "password": null,
  "namespaceId": 1000
}

```

PublishRequest

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|devices|any|false|none|none|

anyOf

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|» *anonymous*|[any]|false|none|none|

or

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|» *anonymous*|null|false|none|none|

continued

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|command|string|true|none|none|
|orgs|any|false|none|none|

anyOf

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|» *anonymous*|[any]|false|none|none|

or

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|» *anonymous*|null|false|none|none|

continued

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|timeout|integer|false|none|none|
|username|string|false|none|none|
|password|string|false|none|none|
|namespaceId|integer|false|none|none|

<h2 id="tocS_RequestResponse">RequestResponse</h2>
<!-- backwards compatibility -->
<a id="schemarequestresponse"></a>
<a id="schema_RequestResponse"></a>
<a id="tocSrequestresponse"></a>
<a id="tocsrequestresponse"></a>

```json
{
  "results": {},
  "error": null,
  "message": null
}

```

RequestResponse

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|results|object|true|none|none|
|error|any|false|none|none|

anyOf

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|» *anonymous*|string|false|none|none|

or

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|» *anonymous*|null|false|none|none|

continued

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|message|string|false|none|none|

<h2 id="tocS_ResultsRequest">ResultsRequest</h2>
<!-- backwards compatibility -->
<a id="schemaresultsrequest"></a>
<a id="schema_ResultsRequest"></a>
<a id="tocSresultsrequest"></a>
<a id="tocsresultsrequest"></a>

```json
{
  "devices": [],
  "requestId": "string",
  "timeout": 60
}

```

ResultsRequest

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|devices|any|false|none|none|

anyOf

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|» *anonymous*|[any]|false|none|none|

or

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|» *anonymous*|null|false|none|none|

continued

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|requestId|string|true|none|none|
|timeout|integer|false|none|none|

