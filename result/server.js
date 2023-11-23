var express = require('express'),
    async = require('async'),
    { Pool } = require('pg'),
    app = express(),
    server = require('http').Server(app),
    io = require('socket.io')(server);

var port = process.env.PORT || 4000;

io.on('connection', function (socket) {
  socket.emit('message', { text : 'Welcome!' });
  socket.on('subscribe', function (data) {
    socket.join(data.channel);
  });
});

var pool = new Pool({
  connectionString: 'postgres://postgres:postgres@db/postgres'
});

async.retry(
  {times: 1000, interval: 1000},
  function(callback) {
    pool.connect(function(err, client, done) {
      if (err) {
        console.error("Waiting for db");
      }
      callback(err, client);
    });
  },
  function(err, client) {
    if (err) {
      return console.error("Giving up");
    }
    console.log("Connected to db");
    getNeighbors(client);
  }
);

// My functions
function getNeighbors(client) {
  client.query('SELECT user_id, neighbor_id FROM neighbors', [], function(err, result) {
    if (err) {
      console.error("Error performing query: " + err);
    } else {
      var neighbors = collectNeighborsFromResult(result);
      io.sockets.emit("neighbors", JSON.stringify(neighbors));
      console.log("Neighbors:", JSON.stringify(neighbors, null, 2));
    }

    setTimeout(function() { getNeighbors(client) }, 1000);
  });
}

function collectNeighborsFromResult(result) {
  var neighbors = {};

  result.rows.forEach(function (row) {
    if (!neighbors[row.user_id]) {
      neighbors[row.user_id] = [];
    }
    neighbors[row.user_id].push(row.neighbor_id);
  });

  return neighbors;
}

app.use(express.static(__dirname + '/views'));

app.get('/', function (req, res) {
  res.sendFile(path.resolve(__dirname + '/views/index.html'));
});

server.listen(port, function () {
  var port = server.address().port;
  console.log('App running on port ' + port);
});