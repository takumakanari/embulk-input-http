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

- **type**: specify this plugin as `http`
- **url**: base url something like api (required)
- **parser**: parser plugin to parse data in response (required)
- **params**: pair of name/value to specify query parameter (optional)
- **method**: http method, get is used by default (optional)
- **user_agent**: the usrr agent to specify request header (optional)
- **charset**: Charset to specify request header (optional, utf-8 is used by default)
- **open_timeout**: timeout msec to open connection (optional, 2000 is used by default)
- **read_timeout**: timeout msec to read content via http (optional, 10000 is used by default)
- **max_retries**: max number of retry request if failed (optional, 5 is used by default)
- **retry_interval**: interval msec to retry max (optional, 10000 is used by default)
- **sleep\_before\_request**: wait msec before each requests (optional, 0 is used by default)

### Brace expansion style in params

In *params* section, you can specify also multilple params by using **brace expansion style**.

```yaml
params
  - {name: id, value "{5,4,3,2,1}", expand: true}
  - {name: name, value "{John,Paul,George,Ringo}", expand: true}
```

To use this style, you need to set true to parameter *expand*, then all patterns of query will be called in a defferent request.


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
- BasicAuth
- HTTP-proxy
- Custom hedaers
- Guess

## Patch

Welcome!
