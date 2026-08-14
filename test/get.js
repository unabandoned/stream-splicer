var test = require('node:test');
var assert = require('node:assert');
var pipeline = require('../');
var through = require('./helpers.js').through;

test('get', function () {
    var a = through.obj();
    var b = through.obj();
    var c = through.obj();

    var pipe = pipeline([ a, b, c ]);
    assert.strictEqual(pipe.get(0), a, '0');
    assert.strictEqual(pipe.get(1), b, '1');
    assert.strictEqual(pipe.get(2), c, '2');
    assert.strictEqual(pipe.get(3), undefined, '3');
    assert.strictEqual(pipe.get(4), undefined, '4');
    assert.strictEqual(pipe.get(5), undefined, '5');
    assert.strictEqual(pipe.get(-1), c, '-1');
    assert.strictEqual(pipe.get(-1), c, '-1');
    assert.strictEqual(pipe.get(-2), b, '-2');
    assert.strictEqual(pipe.get(-3), a, '-3');
    assert.strictEqual(pipe.get(-4), undefined, '-4');
    assert.strictEqual(pipe.get(-5), undefined, '-5');
});

test('nested get', function () {
    var a = through.obj();
    var b = through.obj();
    var c = through.obj();
    var d = through.obj();
    var e = through.obj();
    var f = through.obj();
    var g = through.obj();

    var pipe = pipeline([ a, [ b, c, [ d, [ e ], f ] ], g ]);
    assert.strictEqual(pipe.get(0), a);
    assert.strictEqual(pipe.get(1, -1, 1, 0), e);
    assert.strictEqual(pipe.get(1, 3), undefined);
    assert.strictEqual(pipe.get(4, 3), undefined);
});
