"use strict";

var when = require('when'),
	sequence = require('sequence'),
	aws = require('plata'),
	AWS = require('aws-sdk'),
	util = require('util'),
	events = require('events');

var Scale = function(){
	this.tables = [];
	this.dynamo = null;
	this.checkExceptions = null;
	this.threshold = 20;
	this.interval = 60000;
	this.scaleValue = 1.20;
	this.exceptionCounter = {};
};
util.inherits(Scale, events.EventEmitter);

Scale.prototype.connect = function(config){
	sequence(this).then(function(next){
		aws.connect(config.aws);
		aws.onConnected(next);
	}).then(function(next){
		AWS.config.update({accessKeyId: config.aws.key, secretAccessKey: config.aws.secret});
		AWS.config.update({region: 'us-east-1'});
		this.dynamo = new AWS.DynamoDB();
		this.listTables().then(next);
	}).then(function(next, tb){
		this.tables = tb;
		this.tables.map(function(tableName){
			this.exceptionCounter[tableName] = {
				'read': 0,
				'write': 0
			};
		}.bind(this));
		this.startCounter();
		this.emit('connected');
	});
};

// The actual scaling.

Scale.prototype.scaleThroughput = function(tableName, action, scaleValue){
	var d = when.defer(),
		map = {
			'write': 'WriteCapacityUnits',
			'read': 'ReadCapacityUnits'
		};
	sequence(this).then(function(next){
		this.dynamo.client.describeTable({
			'TableName': tableName
		}).done(function(res) {
			next(res.data.Table.ProvisionedThroughput);
		}).fail(function(err){
			return d.reject(new Error(err));
		});
	}).then(function(next, throughput){
		throughput[map[action]] *= scaleValue;
		this.dynamo.client.updateTable({
			'TableName': tableName,
			'ProvisionedThroughput': throughput
		}).done(function (res) {
			return d.resolve();
		}).fail(function(err){
			return d.reject(new Error(err));
		});
	});
	return d.promise;
};

// Helpers.

Scale.prototype.listTables = function(){
	var d = when.defer();
	this.dynamo.client.listTables().done(function(res){
		d.resolve(res.data.TableNames);
	}).fail(function(err){
		d.reject(new Error(err));
	});
	return d.promise;
};

Scale.prototype.startCounter = function(){
	var self = this;
	this.checkExceptions = setInterval(function(){
		Object.keys(self.exceptionCounter).map(function(tableName){
			var actions = ['read', 'write'];
			actions.forEach(function(action){
				if (self.exceptionCounter[tableName][action] >= self.threshold){
					self.scaleThroughput(tableName, action, self.scaleValue).then(function(){
						console.log('Successfully scaled table ' + tableName + 'by ' + self.scaleValue);
					}, function(){
						throw new Error('Unable to scale table ' + tableName);
					});
				}
			});
		});
	}, this.interval);
};

// CloudWatch stuff, if we decide to use it.

Scale.prototype.checkThroughput = function(){
	var d = when.defer(),
		self = this,
		throughputStats = {};
	when.all(this.tables.map(function(table){
		var p = when.defer();
		self.doThroughputCheck(table).then(function(data){
			throughputStats[table] = data;
			p.resolve();
		}, function(){
			console.log('explode');
			process.exit(0);
			p.reject();
		});
		return p.promise;
	})).then(function(){
		return d.resolve(throughputStats);
	}, function(){
		return d.reject();
	});
	return d.promise;
};

Scale.prototype.doThroughputCheck = function(table){
	var p = when.defer(),
		throughputStats = {
			'write': [],
			'read': []
		},
		self = this,
		timeRangeMinutes = 600,
		startTime = new Date(new Date().getTime() - 1000*60*timeRangeMinutes),
		endTime = new Date(),
		tableName,
		writeCapacity,
		readCapacity;

	sequence().then(function(next){
		self.dynamo.client.describeTable({
			'TableName': table
		})
			.done(next)
			.fail(function(err){
				return p.reject(new Error(err));
			});
	}).then(function(next, res){
		tableName = res.data.Table.TableName;
		writeCapacity = res.data.Table.ProvisionedThroughput.WriteCapacityUnits;
		readCapacity = res.data.Table.ProvisionedThroughput.ReadCapacityUnits;
		self.getThroughputStatistics(tableName,'write', startTime, endTime)
			.then(next, function(err){
				return p.reject(new Error(err));
			});
	}).then(function(next, writeStats){
		if (writeStats.datapoints !== null){
			writeStats.datapoints.member.map(function(pnt){
				throughputStats.write.push({
					'timestamp': pnt.timestamp,
					'value': (pnt.sum/300)/writeCapacity
				});
			});
		}
		self.getThroughputStatistics(tableName,'read', startTime, endTime)
			.then(next, function(err){
				return p.reject(new Error(err));
			});
	}).then(function(next, readStats){
		if (readStats.datapoints !== null){
			readStats.datapoints.member.map(function(pnt){
				throughputStats.read.push({
					'timestamp': pnt.timestamp,
					'value': (pnt.sum/300)/readCapacity
				});
			});
		}
		p.resolve(throughputStats);
	});
	return p.promise;
};

Scale.prototype.getThroughputStatistics = function(tableName, action, startTime, endTime){
	var d = when.defer(),
		metrics = {
			'write': 'ConsumedWriteCapacityUnits',
			'read': 'ConsumedReadCapacityUnits'
		};
	aws.cloudWatch.getMetricStatistics('AWS/DynamoDB', metrics[action],
		300, startTime.toISOString(), endTime.toISOString(),{
			'Sum': '1'
		}, 'Count', {
			'TableName': tableName
		}).then(function(data){
			d.resolve(data.getMetricStatisticsResponse.getMetricStatisticsResult);
		}, function(err){
			d.reject(new Error(err));
		});
	return d.promise;
};

function create(){
	return new Scale();
}

module.exports = {
	'Scale': Scale,
	'create': create
};
