---
layout: docs
title: Configuration
---

# Configuration

You probably don't need to change a lot in this configuration. Even some values may cause undesired behavior, so be careful. This is the reference configuration:

```
flumina {
  bootstrap-brokers = [
    { host: "localhost", port: 9092 }
  ]
  operational-settings {
    retry-backoff = 500 milliseconds
    retry-max-count = 5
    fetch-max-bytes = 131072 # 128 * 1024
    fetch-max-wait-time = 5 milliseconds
    produce-timeout = 1 seconds
    group-session-timeout = 30 seconds
  }
  connections-per-broker = 3
  request-timeout = 30 seconds
}
```

## bootstrap-brokers

This is a array of bootstrap brokers. These brokers can be contacted for bootstrapping the Kafka client. It will discover and connect to the rest of the brokers by it self.

## operational-settings

These are internal settings used by flumina.

- `retry-backoff` - It happens sometimes that some information in Kafka is not up to date yet. Kafka will tell you that in the response and you need to retry again later. The backoff indicates how much time we should wait before retrying again.
- `retry-max-count` - The number of retries flumina will attempt before giving up
- `fetch-max-bytes` - The number of bytes which flumina will fetch per fetch call
- `produce-timeout` - The duration when a produce call will timeout
- `group-session-timeout` - The duration when all group session will timeout

### connections-per-broker

The number of persistent connections per broker flumina will have.

### request-timeout

The duration when a request will time out