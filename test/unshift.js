var test = require('node:test');
var assert = require('node:assert');
var pipeline = require('../');
var through = require('./helpers.js').through;

test('unshift', function () {
    return new Promise(function (resolve, reject) {
        function guard(fn) { try { fn(); } catch (err) { reject(err); } }

        var expected = {};
        expected.a = [ 5, 6 ];
        expected.b = [ 3, 4, 500, 600 ];
        expected.c = [ 13, 14, 510, 610 ];
        expected.output = [ 13/2, 7, 255, 305 ];

        var a = through.obj(function (x, enc, next) {
            var ex = expected.a.shift();
            guard(function () { assert.strictEqual(x, ex, 'a'); });
            this.push(x * 100);
            next();
        });
        var b = through.obj(function (x, enc, next) {
            var ex = expected.b.shift();
            guard(function () { assert.strictEqual(x, ex, 'b'); });
            if (expected.b.length === 2) p.unshift(a)
            this.push(x + 10);
            next();
        });
        var c = through.obj(function (x, enc, next) {
            var ex = expected.c.shift();
            guard(function () { assert.strictEqual(x, ex, 'c'); });
            this.push(x / 2);
            next();
        });

        var p = pipeline.obj([ b, c ]);
        p.pipe(through.obj(function (x, enc, next) {
            var ex = expected.output.shift();
            guard(function () { assert.strictEqual(x, ex); });
            if (expected.output.length === 0) resolve();
            next();
        }));

        p.write(3);
        p.write(4);
        p.write(5);
        p.write(6);
        p.end();
    });
});
