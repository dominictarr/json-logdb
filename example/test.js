var db = require('../')('/tmp/levellog-whatever.log')

db.open(function (err) {
  if(err) throw err

  db.put('key', 'value', function () {
    console.log('written')
  })
})
