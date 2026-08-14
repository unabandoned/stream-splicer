var test = require('node:test');
var assert = require('node:assert');
var pipeline = require('../');
var helpers = require('./helpers.js');
var through = helpers.through;
var stringify = helpers.stringify;
var split = helpers.split;
var concat = helpers.concat;

test('multiunshift', function () {
    return new Promise(function (resolve, reject) {
        var a = split();
        var b = through.obj(function (row, enc, next) {
            this.push(JSON.parse(row));
            next();
        });
        var c = through.obj(function (row, enc, next) { this.push(row.x); next() });
        var d = through.obj(function (x, enc, next) { this.push(x * 111); next() });
        var e = stringify();

        var stream = pipeline();
        stream.unshift(d, e);
        stream.unshift(a, b, c);
        stream.pipe(concat(function (body) {
            try {
                assert.strictEqual(body.toString(), '[\n333\n,\n444\n,\n555\n]\n');
                resolve();
            } catch (err) { reject(err); }
        }));

        stream.write('{"x":3}\n');
        stream.write('{"x":4}\n');
        stream.write('{"x":5}');
        stream.end();
    });
});
