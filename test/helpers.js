'use strict';

// Zero-dependency test helpers built on Node's own stream module. These
// replace the abandoned tape/through2/split/concat-stream/JSONStream dev
// tree the upstream suite relied on, so the fork ships a near-zero dev tree
// while exercising the exact same behaviour.

var Transform = require('stream').Transform;
var Writable = require('stream').Writable;

// through2 stand-in: a Transform whose transform/flush use the classic
// (chunk, enc, next) signature. `through.obj(fn)` is the objectMode variant.
function makeThrough(objectMode) {
    return function (transform, flush) {
        if (typeof transform !== 'function') {
            transform = function (chunk, enc, next) { next(null, chunk); };
        }
        return new Transform({
            objectMode: objectMode,
            transform: transform,
            flush: flush
        });
    };
}

var through = makeThrough(false);
through.obj = makeThrough(true);

// split stand-in: buffer input, emit each newline-delimited line as its own
// object chunk. Matches the `split` module default of dropping the trailing
// empty line.
function split() {
    var buf = '';
    return new Transform({
        readableObjectMode: true,
        transform: function (chunk, enc, next) {
            buf += chunk.toString('utf8');
            var lines = buf.split('\n');
            buf = lines.pop();
            for (var i = 0; i < lines.length; i++) this.push(lines[i]);
            next();
        },
        flush: function (next) {
            if (buf.length) this.push(buf);
            next();
        }
    });
}

// concat-stream stand-in: collect every chunk and hand the caller the
// concatenated Buffer once the stream finishes.
function concat(cb) {
    var chunks = [];
    return new Writable({
        write: function (chunk, enc, next) {
            chunks.push(Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk));
            next();
        },
        final: function (next) {
            cb(Buffer.concat(chunks));
            next();
        }
    });
}

// JSONStream.stringify() stand-in with the default open/sep/close delimiters:
// '[\n', '\n,\n', '\n]\n'.
function stringify() {
    var first = true;
    return new Transform({
        writableObjectMode: true,
        transform: function (row, enc, next) {
            this.push((first ? '[\n' : '\n,\n') + JSON.stringify(row));
            first = false;
            next();
        },
        flush: function (next) {
            this.push(first ? '[\n]\n' : '\n]\n');
            next();
        }
    });
}

module.exports = { through: through, split: split, concat: concat, stringify: stringify };
