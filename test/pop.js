var test = require('node:test');
var assert = require('node:assert');
var pipeline = require('../');
var helpers = require('./helpers.js');
var through = helpers.through;
var split = helpers.split;
var concat = helpers.concat;

test('pop', function () {
    return new Promise(function (resolve, reject) {
        function guard(fn) { try { fn(); } catch (err) { reject(err); } }

        var expected = {};
        expected.replacer = [ '333', '444' ];

        var a = split();
        var b = through.obj(function (row, enc, next) {
            this.push(JSON.parse(row));
            next();
        });
        var c = through.obj(function (row, enc, next) {
            this.push(row.x);
            next();
        });
        var d = through.obj(function (x, enc, next) {
            this.push(String(x * 111));
            next();
        });
        var replacer = through(function (buf, enc, next) {
            var ex = expected.replacer.shift();
            guard(function () { assert.strictEqual(buf.toString(), ex); });
            this.push(buf.toString('hex') + '\n');
            if (expected.replacer.length === 0) {
                stream.pop();
            }
            next();
        });

        var stream = pipeline([ a, b, c, d, replacer ]);
        stream.pipe(concat(function (body) {
            guard(function () {
                assert.strictEqual(body.toString(), '333333\n343434\n555666');
                assert.strictEqual(expected.replacer.length, 0);
            });
            resolve();
        }));

        stream.write('{"x":3}\n');
        stream.write('{"x":4}\n');
        stream.write('{"x":5}\n');
        stream.write('{"x":6}');
        stream.end();
    });
});
