var db = require('../')('/tmp/levellog-whatever.log')
var pull = require('pull-stream')

db.open(function (err) {
  if(err) throw err
  console.log('DB', db)
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

    db.iterator()
    .pipe(pull.drain(console.log))

  })
})
