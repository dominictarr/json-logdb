var fs = require('fs')
var split = require('split')

function delay (fun) {
  return function () {
    var args = [].splice.call(arguments)
    var self = this
    process.nextTick(function () {
      fun.apply(self, args)
    })
    return this
  }
}

/*
function once (fun) {
  var called = false
  return function () {
    if(called) return
    called = true
    fun.apply(this, arguments)
  }
}
*/

module.exports = function (file, cb) {
  var store = {}

  var fd
  var _cbs = []
  var _queue = []
  var queued = false
  var position = 0

  function processQueue() {
    queued = true
    var queue = _queue
    var cbs   = _cbs
    _queue = []; _cbs = []

    var json = new Buffer(queue.map(function (e) {
      return JSON.stringify(e)
    }).join('\n') + '\n')

    if(!fd)
      fs.open(file, 'a', function (err, _fd) {
        if(err)
          return cbs.forEach(function (cb) {
            cb(err)
          })
        fd = _fd
        write()
      })
    else write()

    function write () {
      //we use write directly, because we want this to occur as a batch!
      //also, writing to a stream isn't good enough, because we need to
      //know exactly which writes succeded, and which failed!
      fs.write(fd, json, 0, json.length, position, function (err) {
        if(err)
          return cbs.forEach(function (cb) { cb(err) })

        //if this write succedded, move the cursor forward!
        position += json.length

        //the write succeded! update the store!
        queue.forEach(function (ch) {
          if(ch.type == 'del')
            delete store[ch.key]
          else
            store[ch.key] = ch.value
        })

        //callback to everyone!
        cbs.forEach(function (cb) { cb() })
      })

    }

  }

  function queueWrite(ch, cb) {
    if(Array.isArray(ch))
      ch.forEach(function (ch) {
        _queue.push(ch)
      })
    else _queue.push(ch)

    _cbs.push(cb)

    if(!queued) //or use longer delays?
      process.nextTick(processQueue)
  }

  var ll
  return ll = {
    opened: false,
    open: function (cb) {
      var once = false
      function done (err) {
        if(once) return
        once = true
        if(err) cb(err)
        ll.opened = true
        cb()
      }
      if(ll.opened) delay(done)()
      fs.stat(file, function (err, stat) {
        if(err && err.code == 'ENOENT')
          return cb()

        //if there is already a file, start writing to end
        position = stat.size

        fs.createReadStream(file)
          .pipe(split(/\r?\n/), JSON.parse)
          .on('data', function (data) {
            store[data.key] = data.value
          })
          .on('end',   done)
          .on('error', done)
      })
    },
    get: function (key, cb) {
      if(store[key]) cb(null, store[key])
      else           cb(new Error('key not found'))
      return this
    },
    put: function (key, value, cb) {
      return queueWrite({key: key, value: value, type: 'put'}, cb)
    },
    del: function (key, value, cb) {
      return queueWrite({key: key, value: value, type: 'del'}, cb)
    },
    batch: function (array, cb) {
      return queueWrite(array, cb)
    },
    approximateSize: function (cb) {
      delay(cb)(null, position)
    }
  }
}
