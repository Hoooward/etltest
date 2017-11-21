'use strict'

var SnappyMsgPackStream = require('./index')
var encodeStream = SnappyMsgPackStream.createEncodeStream()
var decodeStream = SnappyMsgPackStream.createDecodeStream()

require('tape')('simple', function (t) {
  var expected = [
    1,
    0,
    // null, // doesn't work
    true,
    'hello',
    'string\nwith\nlines',
    {object: true, name: 'no need for a name'},
    ['this', 'that', {}, [], [null]],
    Math.PI,
    true,
    false,
    Infinity,
    {obj: {}},
    Buffer.from('fööbär'),
    NaN
  ]

  var toSend = expected.slice()
  var expectedItems = expected.slice()
  var actual = []

  encodeStream.pipe(decodeStream).on('data', function (obj) {
    actual.push(obj)
    if (isNaN(obj)) {
      t.assert(isNaN(expectedItems.shift()))
    } else {
      t.deepEqual(obj, expectedItems.shift())
    }
  }).on('end', function () {
    // slicing, as comparing with `NaN` will always equals `false`
    t.deepEqual(actual.slice(0, -1), expected.slice(0, -1))
    t.end()
  })

  while (toSend.length) {
    encodeStream.write(toSend.shift())
  }

  encodeStream.end()
})
