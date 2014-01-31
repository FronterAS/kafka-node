'use strict';

var Cache = function (expire) {

    var self = this;

    self._expire            = expire || 5000; // ms
    self._granularity       = 1000; // ms
    self._store             = {};
    self._recentAddedKeys   = [];
    self._expireQueue       = [];

    setInterval(function () {
        self._expireQueue.push(self._recentAddedKeys);
        self._recentAddedKeys = [];
    }, self._granularity);

    setTimeout(function () {
        self.clean();
    }, self._expire)
};

Cache.prototype.set = function (key, value) {
    this._store[key] = value;
    this._recentAddedKeys.push(key);
};

Cache.prototype.get = function (key) {
    if (this._store[key]) {
        return this._store[key];
    }
};

Cache.prototype.clean = function () {
    var self = this,
        keys = this._expireQueue.shift();

    console.log(this._expireQueue);

    if (keys) {
        keys.forEach(function (key) {
            if (self._store[key]) {
                delete self._store[key];
            }
        });
    }

    // recurrence
    setTimeout(function () {
        self.clean();
    }, this._granularity);
};

Cache.prototype.flush = function () {
    this._store = {}
};

module.exports = Cache;
