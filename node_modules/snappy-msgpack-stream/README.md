# snappy-msgpack-stream

[![Build Status](https://travis-ci.org/KlausTrainer/snappy-msgpack-stream.svg?branch=main)](https://travis-ci.org/KlausTrainer/snappy-msgpack-stream)
[![JavaScript Style Guide](https://img.shields.io/badge/code%20style-standard-brightgreen.svg)](http://standardjs.com/)
[![semantic-release](https://img.shields.io/badge/%20%20%F0%9F%93%A6%F0%9F%9A%80-semantic--release-e10079.svg)](https://github.com/semantic-release/semantic-release)

Streams of framed [Snappy](https://google.github.io/snappy/)-compressed [MessagePack](http://msgpack.org/) messages.

## Usage

``` js
var snappyStream = require('snappy-msgpack-stream');
var encode = snappyStream.createEncodeStream();
var decode = snappyStream.createDecodeStream();

encode.pipe(decode);

decode.on('data', console.log);

encode.write('HELLO');
encode.write({object: true});
encode.write(true);
encode.write({foo: true, bar: 42, baz: null, pow: "wow"});
// encode.write(anyMessagePackSerializableObject);
```

## Limitations

Please note that MessagePack does not support many JavaScript types, like e.g. `Infinity` or `NaN`. However, we use [msgpack-lite](https://www.npmjs.com/package/msgpack-lite), which implements a slightly extended MessagePack protocol, and allows encoding and decoding `Infinity` and `NaN`, as well as `Buffer` instances.
