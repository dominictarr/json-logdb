var db = require('../')('/tmp/levellog-whatever.log')

db.open(function (err) {
  if(err) throw err

  db.put('key', 'value', function () {
    console.log('written')
  })

  db.batch([
    {key: 'A', value: 'apple', type: 'put'},
    {key: 'B', value: 'banana', type: 'put'},
    {key: 'C', value: 'cherry', type: 'put'},
    {key: 'D', value: 'durian', type: 'put'},
    {key: 'E', value: 'elder-berry', type: 'put'},
    {key: 'key', type: 'del'}
  ], function () {

    var it = db.iterator()

    ;(function next() {
      it.next(function (err, item) {
        if(err) throw err
        if(item) {
          console.log(item)
          next()
        }
      })
    })()
    
  })
})
