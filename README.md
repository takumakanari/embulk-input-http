# Embulk::Input::Http

Input HTTP plugin for [Embulk](https://github.com/embulk/embulk).
Fetch data via HTTP.


## Installation

Run this command with your embulk binary.

```ruby
$ embulk gem install embulk-input-http
```

## Usage

Specify in your config.yml file.

```yaml
in:
  type: http
  url: http://express.heartrails.com/api/json
  params:
    - {name: method, value: getStations}
    - {name: x, value: 135.0}
    - {name: y, value: "{30..35}.0", expand: true}
  method: get
```

- **type**: specify this plugin as *http*
- **url**: base url something like api (required)
- **params**: pair of name/value to specify query parameter (optional)
- **pager**: configuration to parameterize paging (optional)
- **method**: http method, get is used by default (optional)
- **user_agent**: the usrr agent to specify request header (optional)
- **request_headers**: the extra request headers as key-value (optional)
- **charset**: charset to specify request header (optional, default: utf8)
- **basic_auth**: username/password for basic authentication (optional)
- **open_timeout**: timeout msec to open connection (optional, default: 2000)
- **read_timeout**: timeout msec to read content via http (optional, default: 10000)
- **max_retries**: max number of retry request if failed (optional, default: 5)
- **retry_interval**: interval msec to retry max (optional, default: 10000)
- **request_interval**: wait msec before each requests (optional, default: 0)
- **interval\_includes\_response\_time**: yes/no, if yes and you set *request_interval*, response time will be included in interval for next request (optional, default: no)
- **direct\_input**: If false, dumps content to temp file first, to avoid read timeout due to process large data while dowoloading from remote (optional, default: true)

### Brace expansion style in 'params'

In *params* section, you can specify also multilple requests by using **values** or **brace expansion style** with set **expand** true.

The configuration using **values** is as below:

```yaml
params:
  - {name: id, values: [5, 4, 3, 2, 1]}
  - {name: name, values: ["John", "Paul", "George", "Ringo"], expand: true}
```

Also You can rewrite this configuration by using **brace expansion style** like as follows:


```yaml
params:
  - {name: id, value "{5,4,3,2,1}", expand: true}
  - {name: name, value "{John,Paul,George,Ringo}", expand: true}
```

Then all patterns of query will be called in a defferent request.

By default, **expand** is false. In this case, all values will be multiple params in one request.

### Use basic authentication

You can specify username/password for basic authentication.

```yaml
basic_auth:
 user: MyUser
 password: MyPassword
```

### Paging by 'pager'

You can configure request parameters for paging requests, like as follows:

```yaml
in:
  type: http
  url: http://express.heartrails.com/api/json
  pager: {from_param: from, to_param: to, start: 1, step: 1000, pages: 10}
```

Properties of pager is as below:

- **from_param**: parameter name of 'from' index
- **to_param**: parameter name of 'to' index (optional)
- **pages**: total page size
- **start**: first index number (optional, 0 is used by default)
- **step**: size to increment (optional, 1 is used by default)

#### Examples of 'pager'

#### Conbination of from/to parameters.

```yaml
pager: {from_param: from, to_param: to, pages: 4, start: 1, step: 10}
```

the request parameters will be:

1. ?from=1&to=10
2. ?from=11&to=20
3. ?from=21&to=30
4. ?from=31&to=40


##### Batch request with incremental page parameter.

```yaml
params:
  - {name: size, value: 100}
pager: {from_param: page, pages: 4, start: 1, step: 1}
```

the request parameters will be:

1. ?page=1&size=100
2. ?page=2&size=100
3. ?page=3&size=100
4. ?page=4&size=100


## Example

### Fetch json via http api

```yaml
in:
  type: http
  url: http://express.heartrails.com/api/json
  params:
    - {name: method, value: getStations}
    - {name: x, value: 135.0}
    - {name: y, value: "{35,34,33,32,31}.0", expand: true}
  request_headers: {X-Some-Key1: some-value1, X-Some-key2: some-value2}
  parser:
    type: json
    root: $.response.station
    schema:
      - {name: name, type: string}
      - {name: next, type: string}
      - {name: prev, type: string}
      - {name: distance, type: string}
      - {name: lat, type: double, path: x}
      - {name: lng, type: double, path: y}
      - {name: line, type: string}
      - {name: postal, type: string}
```

### Fetch csv

```yaml
in:
  type: http
  url: http://192.168.33.10:8085/sample.csv
    - {name: y, value: "{35,34,33,32,31}.0", expand: true}
  parser:
    charset: UTF-8
    newline: CRLF
    type: csv
    delimiter: ','
    quote: '"'
    escape: ''
    skip_header_lines: 1
    columns:
    - {name: id, type: long}
    - {name: account, type: long}
    - {name: time, type: timestamp, format: '%Y-%m-%d %H:%M:%S'}
    - {name: purchase, type: timestamp, format: '%Y%m%d'}
    - {name: comment, type: string}
```

## TODO
- HTTP-proxy
- Guess

## Patch

Welcome!
