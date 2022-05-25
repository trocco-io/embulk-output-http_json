# embulk-output-http_json
[![main](https://github.com/trocco-io/embulk-output-http_json/actions/workflows/main.yml/badge.svg)](https://github.com/trocco-io/embulk-output-http_json/actions/workflows/main.yml)

An embulk output plugin to egest records as json with [`jq`](https://github.com/eiiches/jackson-jq) via http/https.

## Overview

* **Plugin type**: output
* **Load all or nothing**: no
* **Resume supported**: no
* **Cleanup supported**: no

## Configuration

- **scheme**: URI Scheme for the endpoint (string, default: `"https"`, allows: `"https"`, `"http"`)
- **host**: Hostname or IP address of the endpoint (string, required)
- **port**: Port number of the endpoint (integer, optional, allows: `0-65535`)
- **path**: Path of the endpoint (string, optional)
- **headers**: HTTP Headers (array of map, optional, allows: 1 element can contains 1 key-value.)
- **method**: HTTP Method (string, default: `"POST"`, allows: `"GET"`, `"POST"`, `"PUT"`, `"PATCH"`, `"DELETE"`, `"GET"`, `"HEAD"`, `"OPTIONS"`)
- **buffer_size**: The size of input records to put into a request. (integer, default: `100`)
- **fill_json_null_for_embulk_null**: Fill `null` for embulk `null` when building request body json. (boolean, default: `false`)
- **transformer_jq**: jq filter to transform input records. This filter is used for the buffered records that is converted to json array of object. (string, `"."`)
- **success_condition_jq**: jq filter to check whether the response is succeeded or not. You can use [`jq`](https://github.com/eiiches/jackson-jq) to query for the status code and the response body. (string, `".status_code_class == 200"`)
- **retryable_condition_jq**: jq filter to check whether the response is retryable or not. This condition will be used when it is determined that the response is not succeeded by `success_condition_jq`. You can use [`jq`](https://github.com/eiiches/jackson-jq) to query for the status code and the response body. (string, `"true"`)
- **show_request_body_on_error**: Show request body on error. (boolean, default: `true`)
- **maximum_retries**: Maximum retries. (integer, default: `7`)
- **initial_retry_interval_millis**: Initial retry interval in milliseconds. (integer, default: `1000`)
- **maximum_retries_interval_millis**: Maximum retries interval in milliseconds. (integer, default: `60000`)
- **default_timezone**: Default timezone. (string, default: `"UTC"`)
- **default_timestamp_format**: Default timestamp format. (string, default: `"%Y-%m-%d %H:%M:%S %z"`)
- **default_date**: Default date. (string, default: `"1970-01-01"`)
- **logging_interval**: Progress log output interval in seconds. if set `0`, never output progress log. (string, default: `0`)

### About `transformer_jq`

Here is an example to help you understand the JSON that can be queried.

When this plugin get the below configuration,

```
in:
  type: config
  columns:
    - name: col1
      type: string
    - name: col2
      type: long
    - name: col3
      type: double
    - name: col4
      type: json
  values:
    - - ["val1", 1, 1.1, ["val4-1", "val4-2"]]
      - ["val1", 1, 1.1, ["val4-1", "val4-2"]]
      - ["val1", 1, 1.1, ["val4-1", "val4-2"]]
      - ["val1", 1, 1.1, ["val4-1", "val4-2"]]
      - ["val1", 1, 1.1, ["val4-1", "val4-2"]]
      - ["val1", 1, 1.1, ["val4-1", "val4-2"]]
    - - ["val1", 1, 1.1, ["val4-1", "val4-2"]]
      - ["val1", 1, 1.1, ["val4-1", "val4-2"]]
      - ["val1", 1, 1.1, ["val4-1", "val4-2"]]
out:
  type: http_json
  host: example.com
  buffer_size: 5
  transformer_jq: |
    {
      events: [(.[] | {col1: .col1, col2: .col2, col3: .col3, col4-1: .col4[0], col4-2: .col4[1]})],
      events_count: . | length
    }
```

this plugin sends transformed records like below.

```
$ cat <<EOF > /tmp/embulk-output-http_json-example.json
[
  {"col1":"val1", "col2":1, "col3":1.1, "col4":["val4-1","val4-2"]},
  {"col1":"val1", "col2":1, "col3":1.1, "col4":["val4-1","val4-2"]},
  {"col1":"val1", "col2":1, "col3":1.1, "col4":["val4-1","val4-2"]},
  {"col1":"val1", "col2":1, "col3":1.1, "col4":["val4-1","val4-2"]},
  {"col1":"val1", "col2":1, "col3":1.1, "col4":["val4-1","val4-2"]}
]
[
  {"col1":"val1", "col2":1, "col3":1.1, "col4":["val4-1","val4-2"]}
]
[
  {"col1":"val1", "col2":1, "col3":1.1, "col4":["val4-1","val4-2"]},
  {"col1":"val1", "col2":1, "col3":1.1, "col4":["val4-1","val4-2"]},
  {"col1":"val1", "col2":1, "col3":1.1, "col4":["val4-1","val4-2"]}
]
EOF

$ cat /tmp/embulk-output-http_json-example.json | jq '
    {
      events: [(.[] | {col1: .col1, col2: .col2, col3: .col3, "col4-1": .col4[0], "col4-2": .col4[1]})],
      events_count: . | length
    }
'

{
  "events":[
    {"col1":"val1","col2":1,"col3":1.1,"col4-1":"val4-1","col4-2":"val4-2"},
    {"col1":"val1","col2":1,"col3":1.1,"col4-1":"val4-1","col4-2":"val4-2"},
    {"col1":"val1","col2":1,"col3":1.1,"col4-1":"val4-1","col4-2":"val4-2"},
    {"col1":"val1","col2":1,"col3":1.1,"col4-1":"val4-1","col4-2":"val4-2"},
    {"col1":"val1","col2":1,"col3":1.1,"col4-1":"val4-1","col4-2":"val4-2"}
  ],
  "events_count":5
}
{
  "events":[
    {"col1":"val1","col2":1,"col3":1.1,"col4-1":"val4-1","col4-2":"val4-2"}
  ],
  "events_count":1
}
{
  "events":[
    {"col1":"val1","col2":1,"col3":1.1,"col4-1":"val4-1","col4-2":"val4-2"},
    {"col1":"val1","col2":1,"col3":1.1,"col4-1":"val4-1","col4-2":"val4-2"},
    {"col1":"val1","col2":1,"col3":1.1,"col4-1":"val4-1","col4-2":"val4-2"}
  ],
  "events_count":3
}
```

Since each task creates request bodies for each record of `buffer_size`, `transformer_jq` is used in the process.

### Abount `success_condition_jq` and `retryable_condition_jq`

Here is an example to help you understand the JSON that can be queried.

When you gen the below response,

```
$ curl -i -X POST -H "Content-Type: application/json" -d '{"foo": "bar"}' https://example.com/
HTTP/1.1 201 Created

{"message":"success", "affected_rows":1}
```

you can query for the below JSON.

```json
{
  "status_code_class": 200,
  "status_code": 201,
  "response_body": {
    "message": "success",
    "affected_rows": 1
  }
}
```

This JSON is built in this plugin. If there are other parameters you need, please feel free to create an issue.

## Example

```yaml
out:
  type: http_json
  host: example.com
  path: /user/events
  headers:
    - Authorization: Bearer YOUR-API-TOKEN
  method: POST
  buffer_size: 75
  transformer_jq: '{events: [( .[] | {user_id: .user_id, event: .name, time: .time} )]}'
  success_condition_jq: '.response_body.message == "success"'
  default_timestamp_format: "%Y-%m-%dT%H:%M:%S%z"
```

## Development

### Run an example

Firstly, you need to start the mock server.

```shell
$ ./example/run-mock-server.sh
```

then, you run the example.

```shell
$ ./gradlew gem
$ embulk run -Ibuild/gemContents/lib -X min_output_tasks=1 example/config.yml
```

The requested records are shown on the mock server console.

### Run tests

```shell
$ ./gradlew test
```

### Update dependencies locks

```shell
$ ./gradlew dependencies --write-locks
```

### Run the formatter

```shell
## Just check the format violations
$ ./gradlew spotlessCheck

## Fix the all format violations
$ ./gradlew spotlessApply
```

### Release a new gem

A new tag is pushed, then a new gem will be released. See [the Github Action CI Setting](./.github/workflows/main.yml).

## CHANGELOG

[CHANGELOG.md](./CHANGELOG.md)

## License

[MIT LICENSE](./LICENSE.txt)

## Build

```
$ ./gradlew gem  # -t to watch change of files and rebuild continuously
```
