"use strict";

var when = require('when'),
	sequence = require('sequence'),
	aws = require('plata'),
	AWS = require('aws-sdk'),
	getConfig = require('junto'),
	nconf = require('nconf'),
	util = require('util'),
	express = require('express');

var app = express(),
	config,
	dynamo,
	tableInfo = {},
	datapoints;

nconf.file({'file': 'config.json'});

getConfig('development').then(function(c){
	config = c;
	aws.connect(config.aws);
});

// checkThroughput();
// setInterval(checkThroughput, 10000);

// Run with a setInterval for every 5 minutes
// Get CloudWatch data for the last 30 minutes
// If levels for any metric have been about 70% for
// more than 5 consecutive datapoints, trigger scaling

app.get('/notification/:message', function(req, res){

});

app.get('/throughput', function(req, res){
	checkThroughput.then(function(data){
		res.json(data);
	});
});

app.listen(8080);
console.log('Listening on 8080');

function checkThroughput(){
	var d = when.defer(),
		throughputStats = {},
		timeRangeMinutes = 50,
		startTime = new Date(new Date().getTime() - 1000*60*timeRangeMinutes),
		endTime = new Date();

	aws.onConnected(function(){
		AWS.config.update({accessKeyId: config.aws.key, secretAccessKey: config.aws.secret});
		AWS.config.update({region: 'us-east-1'});
		dynamo = new AWS.DynamoDB();

		when.all(nconf.get('tables').map(function(table){
			var p = when.defer();
			dynamo.client.describeTable({
				'TableName': table
			}).done(function(res) {
				var tableName = res.data.Table.TableName;
				throughputStats[tableName] = {
					'write': [],
					'read': []
				};
				// tableInfo[res.data.Table.TableName] = {
				// 	'ProvisionedThroughput': {
				// 		'Read': res.data.Table.ProvisionedThroughput.ReadCapacityUnits,
				// 		'Write': res.data.Table.ProvisionedThroughput.WriteCapacityUnits
				// 	}
				// };
				getThroughputStatistics(tableName,
				'write', startTime, endTime).then(function(writeStats){
					throughputStats[tableName].write =
						(writeStats.datapoints === undefined) ? [] : writeStats.datapoints;
					getThroughputStatistics(res.data.Table.TableName,
						'read', startTime, endTime).then(function(readStats){
							throughputStats[tableName].read =
								(readStats.datapoints === undefined) ? [] : readStats.datapoints;
						});
				});
				p.resolve();
			}).fail(function(err){
				p.reject(new Error(err));
			});
			return p.promise;
		})).then(function(){
			return d.resolve(throughputStats);
		});

	});
	return d.promise;
}

function increaseThroughput(tableName, newThroughput){
	var d = when.defer();
	dynamo.client.updateTable({
		'TableName': tableName,
		'ProvisionedThroughput': newThroughput
	}).done(function (res) {
		return d.resolve();
	}).fail(function(err){
		return d.reject(new Error(err));
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
			// if (datapoints !== undefined){
			// 	datapoints.map(function(pnt){
			// 		var consumed = (pnt.sum/300),
			// 			max = tableInfo[tableName].ProvisionedThroughput.Write;
			// 		console.log(tableName, ' (' + action + '): ', (consumed/max)*100+'% ('+pnt.timestamp+')');
			// 	});
			// }
		});
	return d.promise;
}
