# nsq_to_elasticsearch

Index all messages from a given topic, setting the proper elasticsearch index name (logstash compatible).


    Usage of ./nsq_to_elasticsearch:
      -channel="nsq_to_elasticsearch": nsq channel
      -consumer-opt=: option to passthrough to nsq.Consumer (may be given multiple times, http://godoc.org/github.com/bitly/go-nsq#Config)
      -elasticsearch=: Elasticsearch HTTP address (may be given multiple times)
      -http-timeout=20s: timeout for HTTP connect/read/write (each)
      -index-name="logstash-%Y.%m.%d": elasticsearch index name (strftime format)
      -index-type="logstash": elasticsearch index mapping
      -lookupd-http-address=: lookupd HTTP address (may be given multiple times)
      -max-in-flight=200: max number of messages to allow in flight
      -n=10: number of concurrent publishers
      -nsqd-tcp-address=: nsqd TCP address (may be given multiple times)
      -status-every=250: the # of requests between logging status (per handler), 0 disables
      -topic="": nsq topic
      -version=false: print version string

## Example

- Load balance between 3 elasticsearch instances
- Read from topic `logs`
- Get NSQd instances from `lookupd.local:4161`
- Set index name to `nsq-test-<YYYY>-<MM>-<DD>`


    nsq_to_elasticsearch  -elasticsearch http://es1.local:9200 -elasticsearch http://es2.local:9200 -elasticsearch http://es3.local:9200 -lookupd-http-address lookupd.local:4161 -topic logs -index-name="nsq-test-%Y.%m.%d"
