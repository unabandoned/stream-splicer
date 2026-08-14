var test = require('node:test');
var assert = require('node:assert');
var pipeline = require('../');
var helpers = require('./helpers.js');
var through = helpers.through;
var split = helpers.split;

test('push', function () {
    return new Promise(function (resolve, reject) {
        function guard(fn) { try { fn(); } catch (err) { reject(err); } }

        var expected = {};
        expected.first = [ 333, 444, 555, 666, 777 ];
        expected.second = [ 6.66, 7.77 ];
        expected.output = [ 3.33, 4.44, 5.55, 3, 2 ];

        var a = split();
        var b = through.obj(function (row, enc, next) {
            this.push(JSON.parse(row));
            next();
        });
        var c = through.obj(function (row, enc, next) { this.push(row.x); next() });
        var d = through.obj(function (x, enc, next) { this.push(x * 111); next() });

        var first = through.obj(function (row, enc, next) {
            if (expected.first.length === 2) {
                guard(function () { assert.strictEqual(p.length, 5); });
                p.push(second);
                guard(function () { assert.strictEqual(p.length, 6); });
            }

            var ex = expected.first.shift();
            guard(function () { assert.deepStrictEqual(row, ex); });

            this.push(row / 100);
            next();
        });
        var second = through.obj(function (row, enc, next) {
            var ex = expected.second.shift();
            guard(function () { assert.deepStrictEqual(row, ex); });
            this.push(Math.floor(10 - row));
            next();
        });

        var p = pipeline.obj([ a, b, c, d, first ]);
        guard(function () { assert.strictEqual(p.length, 5); });

        p.pipe(through.obj(function (row, enc, next) {
            var ex = expected.output.shift();
            guard(function () { assert.deepStrictEqual(row, ex); });
            if (expected.output.length === 0) {
                guard(function () {
                    assert.strictEqual(expected.first.length, 0);
                    assert.strictEqual(expected.second.length, 0);
                });
                resolve();
            }
            next();
        }));

        p.write('{"x":3}\n');
        p.write('{"x":4}\n');
        p.write('{"x":5}\n');
        p.write('{"x":6}\n');
        p.write('{"x":7}');
        p.end();
    });
});
