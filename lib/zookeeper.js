'use strict';

var zookeeper        = require('node-zookeeper-client'),
    util             = require('util'),
    EventEmitter      = require('events').EventEmitter;

/**
 * Create a zookeeper client and get/watch brokers.
 * @param connect
 * @constructor
 */
var Zookeeper = function (connectionString) {
    var self = this;

    self.client = zookeeper.createClient(
        connectionString || 'localhost:2181/kafka0.8'
    );

    self.client.on('connected', function () {
        self.listBrokers();
    });

    self.client.connect();
};

util.inherits(Zookeeper, EventEmitter);

Zookeeper.prototype.getBrokerDetail = function (brokerId, cb) {
    var path = '/brokers/ids/' + brokerId;

    this.client.getData(
        path,
        function (error, data) {
            if (error) {
                console.log('Error occurred when getting data: %s.', error);
            }

            if (cb) {
                cb(data);
            }
        }
    );
};

Zookeeper.prototype.listBrokers = function (cb) {
    var self = this,
        path = '/brokers/ids',

        listBrokers = function () {
            self.listBrokers();
        };

    self.client.getChildren(path, listBrokers,

        function (error, children) {
            var errorMessage = 'Failed to list children of node: %s due to: %s.';

            if (error) {
                return console.log(errorMessage, path, error);
            }

            if (children.length) {
                var brokers = {};

                if (!self.inited) {
                    var brokerId = children.shift();

                    self.getBrokerDetail(brokerId, function (data) {
                        brokers[brokerId] = JSON.parse(data.toString());
                        self.emit('init', brokers);
                        self.inited = true;
                        cb && cb(brokers); //For test
                    });

                } else {
                    var count = 0;
                    children.forEach(function (brokerId) {
                        self.getBrokerDetail(brokerId, function (data) {
                            brokers[brokerId] = JSON.parse(data.toString());
                            if (++count == children.length) {
                                self.emit('brokersChanged', brokers)
                                cb && cb(brokers); //For test
                            }
                        })
                    })
                }

            } else {
                if (self.inited) {
                    return self.emit('brokersChanged', {})
                }

                self.inited = true;
                self.emit('init', {});
            }
        }
    );
};

Zookeeper.prototype.topicExists = function (topic, cb, watch) {
    var path = '/brokers/topics/' + topic,
        self = this;
    this.client.exists(
        path,
        function (event) {
            console.log('Got event: %s.', event);

            if (watch) {
                self.topicExists(topic, cb);
            }
        },
        function (error, stat) {
            if (error) return;
            cb(!!stat, topic);
        }
    );
};

module.exports = Zookeeper;
