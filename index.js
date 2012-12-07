"use strict";

var when = require('when'),
	sequence = require('sequence'),
	aws = require('plata'),
	AWS = require('aws-sdk'),
	getConfig = require('junto'),
	util = require('util'),
	express = require('express');

var app = express(),
	config,
	dynamo,
	exceptionCounter = {},
	checkExceptions,
	tables,
	threshold = 20,
	interval = 60000,
	scaleValue = 1.20;

// setup

sequence().then(function(next){
	getConfig('development').then(function(c){
		config = c;
		aws.connect(config.aws);
	});
	aws.onConnected(next);
}).then(function(next){
	AWS.config.update({accessKeyId: config.aws.key, secretAccessKey: config.aws.secret});
	AWS.config.update({region: 'us-east-1'});
	dynamo = new AWS.DynamoDB();
	listTables().then(next);
}).then(function(next, tb){
	tables = tb;
	tables.map(function(tableName){
		exceptionCounter[tableName] = {
			'read': 0,
			'write': 0
		};
	});
	startCounter();
	app.listen(8080);
	console.log('Listening on 8080');
});

app.get('/notification/:message', function(req, res){
	// if this is a throughput exceeded exception,
	// increment the relevant counter
});

app.get('/throughput', function(req, res){
	checkThroughput().then(function(data){
		res.json(data);
	});
});


// We're not really using this,
// but I'm going to keep it
// in here anyways.

function checkThroughput(){
	var d = when.defer(),
		throughputStats = {},
		timeRangeMinutes = 600,
		startTime = new Date(new Date().getTime() - 1000*60*timeRangeMinutes),
		endTime = new Date();

		when.all(tables.map(function(table){
			var p = when.defer();
			dynamo.client.describeTable({
				'TableName': table
			}).done(function(res) {
				var tableName = res.data.Table.TableName;
				throughputStats[tableName] = {
					'write': [],
					'read': []
				};
				getThroughputStatistics(tableName,
					'write', startTime, endTime).then(function(writeStats){
						console.log(writeStats);
					if (writeStats.datapoints !== null){
						writeStats.datapoints.map(function(pnt){
							throughputStats[tableName].write.push({
								'timestamp': pnt.timestamp,
								'value': (pnt.sum/300)/res.data.Table.ProvisionedThroughput.WriteCapacityUnits
							});
						});
					}
					getThroughputStatistics(res.data.Table.TableName,
						'read', startTime, endTime).then(function(readStats){
							if (readStats.datapoints !== null){
								readStats.datapoints.map(function(pnt){
									throughputStats[tableName].read.push({
										'timestamp': pnt.timestamp,
										'value': (pnt.sum/300)/res.data.Table.ProvisionedThroughput.ReadCapacityUnits
									});
								});
							}
							p.resolve();
						});
				});
			}).fail(function(err){
				p.reject(new Error(err));
			});
			return p.promise;
		})).then(function(){
			return d.resolve(throughputStats);
		});
	return d.promise;
}

function getThroughputStatistics(tableName, action, startTime, endTime){
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
		});
	return d.promise;
}

function scaleThroughput(tableName, action, scaleValue){
	var d = when.defer(),
		map = {
			'write': 'WriteCapacityUnits',
			'read': 'ReadCapacityUnits'
		};
	sequence().then(function(next){
		dynamo.client.describeTable({
			'TableName': tableName
		}).done(function(res) {
			next(res.data.Table.ProvisionedThroughput);
		}).fail(function(err){
			return d.reject(new Error(err));
		});
	}).then(function(next, throughput){
		throughput[map[action]] *= scaleValue;
		dynamo.client.updateTable({
			'TableName': tableName,
			'ProvisionedThroughput': throughput
		}).done(function (res) {
			return d.resolve();
		}).fail(function(err){
			return d.reject(new Error(err));
		});
	});
	return d.promise;
}

function listTables(){
	var d = when.defer();
	dynamo.client.listTables().done(function(res){
		d.resolve(res.data.TableNames);
	}).fail(function(err){
		d.reject(new Error(err));
	});
	return d.promise;
}

function startCounter(){
	checkExceptions = setInterval(function(){
		Object.keys(exceptionCounter).map(function(tableName){
			var actions = ['read', 'write'];
			actions.forEach(function(action){
				if (exceptionCounter[tableName][action] >= threshold){
					scaleThroughput(tableName, action, scaleValue).then(function(){
						console.log('Successfully scaled table ' + tableName + 'by ' + scaleValue);
					}, function(){
						throw new Error('Unable to scale table ' + tableName);
					});
				}
			});
		});
	}, interval);
}