'use strict';

var azure = require('azure-storage');

var _tableSvc;

function _clean(data) {
  var result = {};

  for (var prop in data) {
    if (data.hasOwnProperty(prop)) {
      if (prop === '.metadata') continue;

      var value = data[prop]._;
      if (prop === 'Timestamp') value = new Date(value);

      result[prop.substr(0, 1).toLowerCase() + prop.substr(1)] = data[prop]._;
    }
  }

  return result;
}

function Repository(tableName, connectionString) {
  this.tableName = tableName;
  _tableSvc = azure.createTableService(connectionString)
  _tableSvc.createTableIfNotExists(tableName, function() {});
}

Repository.prototype.find = function(partitionKey, rowKey, callback) {
  _tableSvc.retrieveEntity(this.tableName, partitionKey, rowKey, function(error, result, response) {
    callback(error, _clean(result));
  });
};

Repository.prototype.add = function(partitionKey, entity, callback) {
  var batch = new azure.TableBatch();

  for (var prop in entity) {
    if (entity.hasOwnProperty(prop)) {
      var val = entity[prop];

      if (typeof val === 'object') {
        val = JSON.stringify(val);
      }

      batch.insertEntity({
        PartitionKey: {
          '_': partitionKey
        },
        RowKey: {
          '_': prop
        },
        value: {
          '_': val
        }
      });
    }
  }

  _tableSvc.executeBatch(this.tableName, batch, function(error, result, response) {
    callback(error, result);
  });
};

module.exports = Repository;
