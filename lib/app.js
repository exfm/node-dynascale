"use strict";

var Scale = require('./scale'),
	when = require('when'),
	sequence = require('sequence'),
	getConfig = require('junto'),
	express = require('express');

var app = express(),
	scale = Scale.create();

scale.on('connected', function(){
	app.listen(8080);
	console.log('Listening on 8080');
});

scale.on('scale', function(data){
	console.log('Scaled table ' + data.tableName + ' by ' + data.scaledValue);
});

scale.on('err', function(err){
	throw err;
});

getConfig('development').then(function(config){
	scale.connect(config);
});

app.get('/notification/:message', function(req, res){
	// if this is a throughput exceeded exception,
	// increment the relevant counter
});

// Route to get throughput stats in JSON format
app.get('/throughput', function(req, res){
	scale.checkThroughput().then(function(data){
		res.json(data);
	});
});
