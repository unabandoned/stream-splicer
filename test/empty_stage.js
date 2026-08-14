var test = require('node:test');
var assert = require('node:assert');
var pipeline = require('../');
var through = require('./helpers.js').through;

// Regression test. Emptying a nested stage with splice(0) replaces it with a
// passthrough on demand; a readable-stream 2->4 gap left that fresh passthrough
// un-forwarded, so data never reached the pipeline's readable side and the
// stream hung instead of ending. (Surfaced by browserify's "custom packer",
// which does `pipeline.get('wrap').splice(0)`.)
test('splice(0) empties a nested stage without stalling', function () {
    return new Promise(function (resolve, reject) {
        var inner = pipeline.obj([ through.obj(function (r, e, n) { n(null, r); }) ]);
        var main = pipeline.obj([ inner, through.obj(function (r, e, n) { n(null, r); }) ]);

        inner.splice(0); // empty the first stage -> becomes a passthrough

        var out = [];
        main.on('data', function (d) { out.push(String(d)); });
        main.on('error', reject);
        main.on('end', function () {
            try {
                assert.deepStrictEqual(out, [ 'a', 'b', 'c' ]);
                resolve();
            } catch (err) { reject(err); }
        });

        main.write('a');
        main.write('b');
        main.write('c');
        main.end();
    });
});
