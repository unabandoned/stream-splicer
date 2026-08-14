var test = require('node:test');
var assert = require('node:assert');
var pipeline = require('../');
var concat = require('./helpers.js').concat;

test('empty passthrough stream', function () {
    return new Promise(function (resolve, reject) {
        var stream = pipeline([]);
        stream.pipe(concat(function (body) {
            try {
                assert.strictEqual(body.toString(), 'abc');
                resolve();
            } catch (err) { reject(err); }
        }));

        stream.write('a');
        stream.write('b');
        stream.write('c');
        stream.end();
    });
});
