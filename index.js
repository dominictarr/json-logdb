;var fs = require('fs')
var split = require('pull-split')
var pull = require('pull-stream')
var toPull = require('stream-to-pull-stream')

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

module.exports = function (file, cb) {
  var store = {}, _cbs = [], _queue = []
  var fd, queued = false, position = 0

  function processQueue() {
    if(!_queue.length) return queued = false
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
        queued = false
        if(err)
          return cbs.forEach(function (cb) { cb(err) })

        //if this write succedded, move the cursor forward!
        position += json.length

        //the write succeded! update the store!
        queue.forEach(function (ch) {
          if(ch.type == 'del')
            delete store[ch.key]
          else {
            store[ch.key] = ch.value
          }
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

    if(!queued) {//or use longer delays?
      process.nextTick(processQueue)
      queued = true
    }
  }

  var ll
  return ll = {
    opened: false,
    location: file,
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
        if(err)
          return cb(err.code == 'ENOENT' ? null : err)

        //if there is already a file, start writing to end
        position = stat.size

        //TODO: use pull-fs instead.
        pull(
          toPull.source(fs.createReadStream(file)),
          split(/\r?\n/, function (e) { if (e) return JSON.parse(e) }),
          pull.through(function (data) {
            store[data.key] = data.value
          }),
          pull.drain(null, done)
        )
      })
    },
    get: function (key, cb) {
      if(store[key]) cb(null, store[key])
      else           cb(new Error('not found'))
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
    },
    iterator: function (opts) {
      opts = opts || {}
      var snapshot = 
        Object.keys(store).sort().filter(function (k) {
          return (
             (opts.start ? opts.start <= k : true)
          && (opts.end   ? opts.end   >= k : true)
          )
        }).map(function (k) {
          return (
              opts.keys   ? k 
            : opts.values ? store[k] 
            :               {key: k, value: store[k]}
          )
        })

      if(opts.reverse)
        snapshot.reverse()

      return pull.values(snapshot)

    },
    createReadStream: function (opts) { return ll.iterator(opts) }
  }
}
