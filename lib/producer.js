'use strict';

var util               = require('util'),
    events             = require('events'),
    _                  = require('lodash'),
    Client             = require('./client'),
    protocol           = require('./protocol'),
    Message            = protocol.Message,
    ProduceRequest     = protocol.ProduceRequest,

    DEFAULTS           = {
        requireAcks: 1,
        ackTimeoutMs: 100
    };

var Producer = function (client) {
    this.ready  = false;
    this.client = client;

    this.buildOptions(Array.prototype.slice.call(arguments, 2));
    this.connect();
}

util.inherits(Producer, events.EventEmitter);

Producer.prototype.buildOptions = function (args) {
    this.requireAcks  = DEFAULTS.requireAcks;
    this.ackTimeoutMs = DEFAULTS.ackTimeoutMs;
}

Producer.prototype.connect = function () {
    // emitter...
    var self = this;
    self.ready = self.client.ready;

    if (self.ready) {
        self.emit('ready');
    }

    self.client.on('ready', function () {
        if (!self.ready) {
            self.emit('ready');
        }
        self.ready = true;
    });

    self.client.on('error', function (err) {
        self.emit('error');
    });

    self.client.on('close', function () {
        self.emit('close');
    });
}

Producer.prototype.send = function (payloads, cb) {
    this.client.sendProduceRequest(
        this.buildPayloads(payloads),
        this.requireAcks,
        this.ackTimeoutMs,
        cb
    );
}

Producer.prototype.buildPayloads = function (payloads) {
    return payloads.map(function (p) {
        var messages = _.isArray(p.messages) ? p.messages : [p.messages];

        p.partition = p.partition || 0;

        messages = messages.map(function (message) {
            return new Message(0, 0, '', message);
        });

        return new ProduceRequest(p.topic, p.partition, messages);
    });
}

Producer.prototype.createTopics = function (topics, async, cb) {
    var self = this;

    if (!self.ready) {
        setTimeout(function () {
            self.createTopics(topics, async, cb);
        }, 100);
        return;
    }

    topics = typeof topic === 'string' ? [topics] : topics;

    if (typeof async === 'function' && typeof cb === 'undefined') {
        cb = async;
        async = true;
    }

    // first, load metadata to create topics
    self.client.loadMetadataForTopics(topics, function (err, resp) {
        if (async) {
            return cb && cb(null, 'All requests sent');
        }

        var topicMetadata = resp[1].metadata;

        // omit existed topics
        var topicsNotExists =
            _.pairs(topicMetadata)
                .filter(function (pairs) {
                    return _.isEmpty(pairs[1])
                })
                .map(function (pairs) {
                    return pairs[0]
                });

        if (!topicsNotExists.length) {
            return cb && cb(null, 'All created');
        }

        // check from zookeeper to make sure topic created
        self.client.createTopics(topicsNotExists, function (err, created) {
            cb && cb(err, created);
        });
    });
}

function noAcks() {
    return 'Not require ACK';
}

module.exports = Producer;
