# Embulk::Input::Http

Input HTTP plugin for [Embulk](https://github.com/embulk/embulk).
Read content via HTTP and parse/iterate json(or xml) data.

## Installation

Run this command with your embulk binary.

```ruby
$ embulk gem install embulk-input-http
```

## Usage

Specify in your config.yml file

```yaml
in:
  type: http
  url: http://express.heartrails.com/api/json
  params:
    - {name: method, value: getStations}
    - {name: x, value: 135.0}
    - {name: y, value: "{30..35}.0", expand: true}
  schema:
    - {name: name, type: string}
    - {name: next, type: string}
    - {name: prev, type: string}
    - {name: distance, type: string}
    - {name: lat, type: double, path: x}
    - {name: lng, type: double, path: y}
    - {name: line, type: string}
    - {name: postal, type: string}
  iterate: {type: json, path: $.response.station}
  method: get
```

- type: specify this plugin as `http`
- url: base url something like api (required)
- schema: specify the attribute of table and data type (required)
- iterate: data type and path to find root data, json/xml is supported for now (required)
- method: http method, get is used by default (optional)
- params: pair of name/value to specify query parameter (optional)
- open_timeout: timeout to open connection (optional, 5 is used by default)
- read_timeout: timeout to read content via http (optional, 10 is used by default)

### Iterate data

You can specify 2 types to parse result from HTTP api in *iterate* section.


#### json

For this type, you need to specify *path* as  [jsonpath](http://goessner.net/articles/JsonPath/).

for example:

```json
{
    "result" : "success",
    "students" : [
      { "name" : "John", "age" : 10 },
      { "name" : "Paul", "age" : 16 },
      { "name" : "George", "age" : 17 },
      { "name" : "Ringo", "age" : 18 }
    ]
}
```

You can iterate "students" node by the following condifuration:

    iterate: {type: json, path: $.students}

You can specify jsonpath to also *path* in schema section:

```yaml
schema:
  - {name: firstName, type: string, path: "names[0]"}
  - {name: lastName, type: string, path: "names[1]"}
iterate: {type: json, path: $.students}
```

Then you can make record from more complicated json like as follows:

```json
{
    "result" : "success",
    "students" : [
      { "names" : ["John", "Lennon"], "age" : 10 },
      { "names" : ["Paul", "Maccartney"], "age" : 10 }
    ]
}
```

In this case, names[0] will be firstName of schema and names[1] will be lastName.


#### xml

You can parse also xml by specifing **path/to/node** style to *path*.

for example:


```xml
<data>
  <result>true</result>
  <students>
    <student>
      <name>John</name>
      <age>10</name>
    <student>
    <student>
      <name>Paul</name>
      <age>16</name>
    <student>
    <student>
      <name>George</name>
      <age>17</name>
    <student>
    <student>
      <name>Ringo</name>
      <age>18</name>
    <student>
  </students>
```

Configuration as below to iterate student node:

    iterate: {type: xml, path: data/students/student}

### Brace expansion style in params

In *params* section, you can specify also multilple params by using **brace expansion style**.

```yaml
params
  - {name: id, value "{1..5}", expand: true}
  - {name: name, value "{John,Paul,George,Ringo}", expand: true}
```

To use this style, you need to set true to parameter *expand*, then all patterns of query will be called in a defferent request.


## TODO
- Split input/formatter
- BasicAuth
- HTTP-proxy

## Patch

Welcome!
