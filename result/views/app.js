var app = angular.module('catsvsdogs', []);
var socket = io.connect();

app.controller('statsCtrl', function ($scope) {
  var updateNeighbors = function () {
    socket.on('neighbors', function (json) {
      console.log('Received neighbors:', json);
      var data = JSON.parse(json);

      $scope.neighbors = data;

      console.log('$scope.neighbors:', $scope.neighbors);
      $scope.$apply();
    });
  };

  var init = function () {
    console.log('Initializing...');
    document.body.style.opacity = 1;
    updateNeighbors();
  };

  socket.on('message', function (data) {
    init();
  });
});