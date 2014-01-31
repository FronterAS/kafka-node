'use strict';

var kafka = require('../kafka'),
    Consumer = kafka.Consumer,

    Offset = kafka.Offset,
    Client = kafka.Client,
    argv = require('optimist').argv,

    topic = {
        topic: 'Unicorns',
        partition: '0', //default 0
        offset: 0, //default 0
    },

    client = new Client('mq.dev.fronter.net:2181/'),

    topics = [
        {topic: topic, partition: 1}
    ],

    options = {
        groupId: 'kafka-node-group',//consumer group id, deafult `kafka-node-group`
        // Auto commit config
        autoCommit: true,
        autoCommitIntervalMs: 5000,
        // The max wait time is the maximum amount of time in milliseconds to block waiting if insufficient data is available at the time the request is issued, default 100ms
        fetchMaxWaitMs: 100,
        // This is the minimum number of bytes of messages that must be available to give a response, default 1 byte
        fetchMinBytes: 1,
        // The maximum bytes to include in the message set for this partition. This helps bound the size of the response.
        fetchMaxBytes: 1024 * 10,
        // If set true, consumer will fetch message from the given offset in the payloads
        fromOffset: false
    };

function createConsumer(topics) {
    var consumer = new Consumer(client, topics, options),
        offset = new Offset(client);

    consumer.on('message', function (message) {
        console.log(this.id, message);
    });

    consumer.on('error', function (err) {
        console.log('error', err);
    });

    consumer.on('offsetOutOfRange', function (topic) {
        topic.maxNum = 2;
        offset.fetch([topic], function (err, offsets) {
            var min = Math.min.apply(
                null,
                offsets[topic.topic][topic.partition]
            );

            consumer.setOffset(topic.topic, topic.partition, min);
        });
    });
}

createConsumer(topics);
