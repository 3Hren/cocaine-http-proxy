# Cocaine HTTP proxy

An entry point to the Cocaine Cloud.

Cocaine HTTP proxy terminates HTTP traffic and transforms it into the binary protocol, allowing to communicate with Cloud services and applications.

### Features

##### High performance and low memory footprint.
Backed with [Cocaine Framework Rust][cocaine-framework-rust] and [MessagePack][rmp] this HTTP proxy allows to communicate asynchronously with the Cloud applications and services.
    
##### Cloud Logging.
...

##### Metrics
...

##### Tracing
The proxy is aware of Google Dapper tracing mechanism. Each request is marked with three special internal headers: **trace_id**, **span_id** and **parent_id**, which are transported with it, allowing to build full tracing path to ease debugging.
  
...
  
##### JSON-RPC

### Examples

[rmp]: http://
[cocaine-framework-rust]: http://
