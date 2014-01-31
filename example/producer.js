'use strict';

var kafka = require('../kafka'),
    Producer = kafka.Producer,
    Client = kafka.Client,
    client = new Client('mq.dev.fronter.net:2181/'),
    producer = new Producer(client),

var argv = require('optimist').argv;

var topic = argv.topic || 'Unicorns';

var p = argv.p || 0;

var count = argv.count || 1, rets = 0,

    onSent = function (err/*, data*/) {
        if (err) {
            console.log(arguments);
        }

        rets += 1;

        if (rets === count) {
            process.exit();
        }
    },

    send = function (message) {
        for (var i = 0; i < count; i += 1) {
            producer.send([
                {
                    topic: topic,
                    messages: [message],
                    partition: p
                }
            ], onSent);
        }
    };

producer.on('ready', function () {
    console.log('woop woop');
    send('hello');
});

