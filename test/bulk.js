var pull   = require('pull-stream')
var tape   = require('tape')
var Logdb  = require('../')
var path   = require('path')
var tmpdir = require('osenv').tmpdir()
var fs     = require('fs')

function sortKeys(obj) {
  var o = {}
  Object.keys(obj).sort().forEach(function (key) {
    o[key] = obj[key]
  })
  return o
}

function isSorted() {
  var max
  return pull.map(function (data) {
    if(!max) 
      max = data.key
    else if(data.key < max) throw new Error('out of order')
    return data
  })
}


function create(name, cb) {
  var filename = path.join(tmpdir, name)
  fs.unlink(filename, function () {
    var db = Logdb(filename)
    db.open(cb)
  })
}

tape('dump lots of data', function (t) {

  create('test-logdb-bulk', function (err, db) {
    if(err) throw err

    var input = {}
    pull(
      pull.count(10000),
      pull.map(function (e) {
        return { key: '*' + Math.random(), value: {count: e, ts: Date.now()} }
      }),
      pull.asyncMap(function (e, cb) {
        input[e.key] = e.value
        db.put(e.key, e.value, cb)
      }),
      pull.drain(null, function (err) {
        if(err) throw err
        input = sortKeys(input)
        var output = {}
        console.log('now read...')
        pull(
          db.createReadStream(),
          isSorted(),
          pull.through(function (data) {
            if(Math.random() < 0.01)
              console.log(data.key)
            output[data.key] = data.value
          }),
          pull.drain(null, function () {
            console.log('done')
            t.deepEqual(output, input)
            console.log('done')
            t.end()
          })
        )
      })
    )
  })
})
