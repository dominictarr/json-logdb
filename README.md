# json-logdb

Pure js clone of a leveldb log.

A small in-memory db, with the same api as [leveldown](https://github.com/rvagg/node-leveldown),
to be combined with [https://github.com/dominictarr/json-sst] to form a pure js leveldb clone.

All data is stored as line separated json, so to avoid tricky binary stuff.
It is intended this will be replaced once [https://github.com/rvagg/node-leveljs](leveljs) is ready.

## License

MIT
