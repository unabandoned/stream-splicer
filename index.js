var Duplex = require('readable-stream').Duplex;
var PassThrough = require('readable-stream').PassThrough;
var Readable = require('readable-stream').Readable;
var inherits = require('inherits');

var nextTick = typeof setImmediate !== 'undefined'
    ? setImmediate : process.nextTick
;

module.exports = Pipeline;
inherits(Pipeline, Duplex);

module.exports.obj = function (streams, opts) {
    if (!opts && !Array.isArray(streams)) {
        opts = streams;
        streams = [];
    }
    if (!streams) streams = [];
    if (!opts) opts = {};
    opts.objectMode = true;
    return new Pipeline(streams, opts);
};

function Pipeline (streams, opts) {
    if (!(this instanceof Pipeline)) return new Pipeline(streams, opts);
    if (!opts && !Array.isArray(streams)) {
        opts = streams;
        streams = [];
    }
    if (!streams) streams = [];
    if (!opts) opts = {};
    Duplex.call(this, opts);
    
    var self = this;
    this._options = opts;
    this._wrapOptions = { objectMode: opts.objectMode !== false };
    this._streams = [];
    this._forwarding = null;

    // readable-stream 2 delivered the tail's output one pushed chunk at a
    // time, which the original lazy `read()` pull relied on. readable-stream
    // 3+/Node coalesce a binary readable's buffered chunks on `read()`, which
    // both merges write boundaries (nested pipelines) and strands whatever the
    // tail had buffered when it was spliced out (pop/splice). Forwarding the
    // tail via its per-chunk `data` events preserves boundaries and keeps the
    // data moving; backpressure pauses the tail when our own push fills up.
    this._onData = function (chunk) {
        if (Duplex.prototype.push.call(self, chunk) === false && self._forwarding) {
            self._forwarding.pause();
        }
    };
    this.on('_mutate', function () {
        self._forward();
    });

    this.splice.apply(this, [ 0, 0 ].concat(streams));

    this.once('finish', function () {
        self._notEmpty();
        self._streams[0].end();
    });
}

Pipeline.prototype._read = function () {
    this._notEmpty();
    this._forward();
};

Pipeline.prototype._forward = function () {
    var tail = this._streams[this._streams.length - 1];
    if (this._forwarding === tail) {
        // Still forwarding the same tail; just make sure it keeps flowing.
        if (tail) tail.resume();
        return;
    }
    // The tail changed (splice/pop/push) or the pipeline emptied — always
    // detach the previous forwarder, even when there is no new tail yet, so a
    // spliced-out stream never keeps feeding our output.
    this._unforward();
    if (!tail) return;
    this._forwarding = tail;
    tail.on('data', this._onData);
    tail.resume();
};

Pipeline.prototype._unforward = function () {
    var prev = this._forwarding;
    if (!prev) return;
    prev.removeListener('data', this._onData);
    this._forwarding = null;

    if (this._streams.indexOf(prev) === -1) {
        // `prev` was spliced out of the pipeline (e.g. pop()): its buffered
        // output would otherwise be stranded, so flush it to our output.
        var chunk;
        while ((chunk = prev.read()) !== null) {
            Duplex.prototype.push.call(this, chunk);
        }
    } else {
        // `prev` is still in the pipeline, just no longer the tail; it has been
        // re-piped to its new successor, so let that pipe carry its data.
        prev.resume();
    }
};

Pipeline.prototype._write = function (buf, enc, next) {
    this._notEmpty();
    this._streams[0]._write(buf, enc, next);
};

Pipeline.prototype._notEmpty = function () {
    var self = this;
    if (this._streams.length > 0) return;
    var stream = new PassThrough(this._options);
    stream.once('end', function () {
        var ix = self._streams.indexOf(stream);
        if (ix >= 0 && ix === self._streams.length - 1) {
            Duplex.prototype.push.call(self, null);
        }
    });
    this._streams.push(stream);
    this.length = this._streams.length;
    // The pipeline was empty and just gained a passthrough tail (e.g. after
    // splice(0)); re-establish forwarding so its output reaches our readable
    // side. `_notEmpty` is also reached before any tail exists, hence the guard.
    this.emit('_mutate');
};

Pipeline.prototype.push = function (stream) {
    var args = [ this._streams.length, 0 ].concat([].slice.call(arguments));
    this.splice.apply(this, args);
    return this._streams.length;
};

Pipeline.prototype.pop = function () {
    return this.splice(this._streams.length-1,1)[0];
};

Pipeline.prototype.shift = function () {
    return this.splice(0,1)[0];
};

Pipeline.prototype.unshift = function () {
    this.splice.apply(this, [0,0].concat([].slice.call(arguments)));
    return this._streams.length;
};

Pipeline.prototype.splice = function (start, removeLen) {
    var self = this;
    var len = this._streams.length;
    start = start < 0 ? len - start : start;
    if (removeLen === undefined) removeLen = len - start;
    removeLen = Math.max(0, Math.min(len - start, removeLen));
    
    for (var i = start; i < start + removeLen; i++) {
        if (self._streams[i-1]) {
            self._streams[i-1].unpipe(self._streams[i]);
        }
    }
    if (self._streams[i-1] && self._streams[i]) {
        self._streams[i-1].unpipe(self._streams[i]);
    }
    var end = i;
    
    var reps = [], args = arguments;
    for (var j = 2; j < args.length; j++) (function (stream) {
        if (Array.isArray(stream)) {
            stream = new Pipeline(stream, self._options);
        }
        stream.on('error', function (err) {
            err.stream = this;
            self.emit('error', err);
        });
        stream = self._wrapStream(stream);
        stream.once('end', function () {
            var ix = self._streams.indexOf(stream);
            if (ix >= 0 && ix === self._streams.length - 1) {
                Duplex.prototype.push.call(self, null);
            }
        });
        reps.push(stream);
    })(arguments[j]);
    
    for (var i = 0; i < reps.length - 1; i++) {
        reps[i].pipe(reps[i+1]);
    }
    
    if (reps.length && self._streams[end]) {
        reps[reps.length-1].pipe(self._streams[end]);
    }
    if (reps[0] && self._streams[start-1]) {
        self._streams[start-1].pipe(reps[0]);
    }
    
    var sargs = [start,removeLen].concat(reps);
    var removed = self._streams.splice.apply(self._streams, sargs);
    
    for (var i = 0; i < reps.length; i++) {
        reps[i].read(0);
    }
    
    this.emit('_mutate');
    this.length = this._streams.length;
    return removed;
};

Pipeline.prototype.get = function () {
    if (arguments.length === 0) return undefined;
    
    var base = this;
    for (var i = 0; i < arguments.length; i++) {
        var index = arguments[i];
        if (index < 0) {
            base = base._streams[base._streams.length + index];
        }
        else {
            base = base._streams[index];
        }
        if (!base) return undefined;
    }
    return base;
};

Pipeline.prototype.indexOf = function (stream) {
    return this._streams.indexOf(stream);
};

Pipeline.prototype._wrapStream = function (stream) {
    if (typeof stream.read === 'function') return stream;
    var w = new Readable(this._wrapOptions).wrap(stream);
    w._write = function (buf, enc, next) {
        if (stream.write(buf) === false) {
            stream.once('drain', next);
        }
        else nextTick(next);
    };
    return w;
};
