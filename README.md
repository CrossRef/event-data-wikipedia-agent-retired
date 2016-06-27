# Event Data Wikipedia Agent

Event Data for polling Wikipedia articles and extracting DOI citations that are added and removed.

Follows the [Baleen](https://github.com/crossref/baleen) framework, therefore requires:

 - the "DOI Destinations" API.
 - a Redis instance.
 - a Lagotto instance.
 - Amazon S3.

Required config keys:

    :doi-destinations-base-url
    :archive-s3-bucket
    :s3-access-key-id
    :s3-secret-access-key
    :redis-host
    :redis-port
    :redis-db-number
    :monitor-port
    :lagotto-api-base-url
    :lagotto-source-token
    :lagotto-auth-token
    :recent-changes-subscribe-filter


## To run

Several processes need to be run:

### Ingester

Read live stream from Wikipedia.

    lein with-profile prod run ingest

### Processor

Process diffs to extract events. Run two or more of these.

    lein with-profile prod run process

### Pusher

Push events to Lagotto

    lein with-profile prod run push

### Monitor

Monitor data throughput, serve status page.

    lein with-profile prod run monitor

## Install on production

`lein uberjar` to compile. Systemd scripts in `etc`:

 - `event-data-wikipedia-agent-ingest.service`
 - `event-data-wikipedia-agent-monitor.service`
 - `event-data-wikipedia-agent-process.service`
 - `event-data-wikipedia-agent-push.service`


## License

Copyright © 2016 FIXME

Distributed under the Eclipse Public License either version 1.0 or (at
your option) any later version.
