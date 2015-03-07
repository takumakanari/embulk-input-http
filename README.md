# Embulk::Input::Http

Input HTTP plugin for [Embulk](https://github.com/embulk/embulk).
Read content via HTTP and parse/iterate json(or xml) data.

## Installation

Run this command with your embulk binary.

```ruby
$ embulk gem install embulk-plugin-input-http
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
    - {name: y, value: 35.0}
  schema:
    - {name: name, type: string}
    - {name: next, type: string}
    - {name: prev, type: string}
    - {name: distance, type: string}
    - {name: x, type: double}
    - {name: y, type: double}
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


## TODO

- BasicAuth
- HTTP-proxy
- Breace-expansion style parameter, such as curl

## Patch

Welcome!
