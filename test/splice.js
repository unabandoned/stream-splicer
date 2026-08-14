var test = require('node:test');
var assert = require('node:assert');
var pipeline = require('../');
var helpers = require('./helpers.js');
var through = helpers.through;
var split = helpers.split;
var concat = helpers.concat;

test('splice', function () {
    return new Promise(function (resolve, reject) {
        function guard(fn) { try { fn(); } catch (err) { reject(err); } }

        var expected = {};
        expected.replacer = [ '333', '444', '5000', '6000' ];
        expected.d = [ 3, 4 ];
        expected.thousander = [ 5, 6 ];

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
            guard(function () { assert.strictEqual(x, expected.d.shift(), 'd'); });
            this.push(String(x * 111));
            next();
        });
        var thousander = through.obj(function (x, enc, next) {
            guard(function () { assert.strictEqual(x, expected.thousander.shift(), 'thousander'); });
            this.push(String(x * 1000));
            next();
        });

        var replacer = through(function (buf, enc, next) {
            var ex = expected.replacer.shift();
            guard(function () { assert.strictEqual(buf.toString(), ex); });
            if (expected.replacer.length === 2) {
                stream.splice(3, 1, thousander);
            }
            this.push(buf.toString('hex') + '\n');
            next();
        });

        var stream = pipeline([ a, b, c, d, replacer ]);
        stream.pipe(concat(function (body) {
            guard(function () {
                assert.strictEqual(
                    body.toString(),
                    '333333\n343434\n35303030\n36303030\n'
                );
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
