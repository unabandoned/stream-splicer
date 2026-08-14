var test = require('node:test');
var assert = require('node:assert');
var pipeline = require('../');
var concat = require('./helpers.js').concat;

test('empty with no data', function () {
    return new Promise(function (resolve, reject) {
        var stream = pipeline([]);
        stream.end();
        stream.pipe(concat(function (body) {
            try {
                assert.strictEqual(body.toString(), '');
                resolve();
            } catch (err) { reject(err); }
        }));
    });
});
