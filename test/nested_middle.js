var test = require('node:test');
var assert = require('node:assert');
var pipeline = require('../');
var helpers = require('./helpers.js');
var through = helpers.through;
var split = helpers.split;
var concat = helpers.concat;

test('nested middle splicer', function () {
    return new Promise(function (resolve, reject) {
        var addNewLines = through(function (buf, enc, next) {
            this.push(buf + '\n');
            next();
        });

        var stream = pipeline.obj([
            through.obj(function (str, enc, next) {
                this.push(str.replace(/^./, function (c) {
                    return String.fromCharCode(c.charCodeAt(0) + 5);
                }));
                next();
            }),
            [ split(), addNewLines ],
            through(function (buf, enc, next) {
                this.push('> ' + buf);
                next()
            })
        ]);

        stream.get(1).unshift(through(function (buf, enc, next) {
            this.push(buf.toString('utf8').toUpperCase());
            next();
        }));

        stream.pipe(concat(function (body) {
            try {
                assert.strictEqual(body.toString(), '> F\n> G\n> H\n');
                resolve();
            } catch (err) { reject(err); }
        }));

        stream.write('a\n');
        stream.write('b\n');
        stream.write('c');
        stream.end();
    });
});
