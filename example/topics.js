var kafka = require('../kafka');
var Consumer = kafka.Consumer;
var Producer = kafka.Producer;
var Client = kafka.Client;

var client = new Client('mq.dev.fronter.net:2181/');

function createTopics() {
    var producer = new Producer(client);
    producer.createTopics(['Unicorns'],false, function (err, data) {
        console.log(data);
    });
}

function createTopic(topicName, async) {
    console.log('Creating a topic');
    var producer = new Producer(client);

    producer.createTopics(
        [topicName],
        !!async || false,
        function (err, data) {
            if (err) {
                console.log(err);
            }

            console.log(data);
        }
    );
}

function addTopics() {
    var consumer = new Consumer(client,[{topic: 'Unicorns'}]);
    consumer.on('message', function (msg) { console.log(msg) });

    consumer.addTopics(
        [{topic: 'topic5'},
        {topic: 'topic4'},
        {topic: 'topic3'}],

        function (err, data) {
            if (err) console.log(err);
            console.log(data);}
    );
}

function removeTopics() {
    var consumer = new Consumer(
        client,
        [{topic: 'Unicorns'}]
    );

    consumer.removeTopics(['Unicorns'], function (err, data) {
        console.log(data);
    });
}

function exit() {
    consumer.commit(function (err, data) {
        console.log(data);
        process.exit();
    });
}

//addTopics();

//setTimeout(removeTopics, 5000);

// createTopics();
// removeTopics();

createTopic('Unicorns');
