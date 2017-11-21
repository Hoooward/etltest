'use strict'

var pumpify = require('pumpify')
var through = require('through2')
var lpStream = require('length-prefixed-stream')
var msgpack = require('msgpack-lite')
var snappy = require('snappyjs')

module.exports.createEncodeStream = function SnappyMsgpackEncodeStream (stream) {
  var msgEncode = through.obj(function (data, enc, next) {
    next(null, msgpack.encode(data))
  })

  var snappyCompress = through.obj(function (data, enc, next) {
    next(null, snappy.compress(data))
  })

  return pumpify.obj(msgEncode, snappyCompress, lpStream.encode())
}

module.exports.createDecodeStream = function SnappyMsgpackDecodeStream (stream) {
  var msgDecode = through.obj(function (data, enc, next) {
    next(null, msgpack.decode(data))
  })

  var snappyUncompress = through.obj(function (data, enc, next) {
    next(null, snappy.uncompress(data))
  })

  return pumpify.obj(lpStream.decode(), snappyUncompress, msgDecode)
}
