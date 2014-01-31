'use strict';

var kafka = require('../kafka');
var Client = kafka.Client;
var client = new Client('mq.dev.fronter.net:2181/');
var total = 50;
var assert = require('assert');
var count = 0;

var test = function () {
    console.log('Fetching');
    console.time('fetch');

    for (var i = 0; i < total; i += 1) {
        fetch(function (err, data) {
            count += 1;

            if (count === total) {
                console.timeEnd('fetch');
                console.log(data[1].metadata.Unicorns);
            }
        });
    }
};

var fetch = function (cb) {
    console.time('fetching');
    client.loadMetadataForTopics(['Unicorns'], function (err, data) {
        console.log(data[1].metadata.Unicorns);
    });
};

client.on('error', function () {
    console.log('error');
    console.log(arguments);

}).on('ready', function () {
    console.log('I am ready');
    fetch();
});
