// Scale, the object responsible for dynascale tasks.

// sort of inspired by the way TCP congestion control works but a lot simpler (for now).

// basically, dynascale will run a background task on a 60-second window. the API will present
// a route to be hit by SNS when a throughput-exceeded exception occurs, with a timestamp, tablename
// and operation (read/write).  after 60 second, the exceptions per second (eps) will be calculated
// for every table.  the 60 eps datapoints will be analysed (somehow) and if there's a rising
// trend above a certain threshold, trigger upscaling of through for that table/operation.

// the data analysis could be done with https://github.com/stackd/gauss. for a first pass,
// it could be something pretty simple, just check if the trend is increasing overall over
// the 60 second period.  we should pull the scale-up percentage from the eps data analysis
// stage - related to slope, perhaps?  i'm not sure if the eps will be linear or exponential
// but we need a way to recognize those cases.

// 1. get exceptions-per-second (60 pts per window)
// 2. derivative of the array
// 3. use gauss.vector to do some analysis (stddev, etc) to see if it's increasing with time
// 4. scale!!!!!!!

"use strict";

var when = require('when'),
    sequence = require('sequence'),
    aws = require('plata'),
    util = require('util'),
    calc = require('./calc'),
    events = require('events'),
    gauss = require('gauss');

var Scale = function(){
    this.tables = [];
    this.dynamo = null;
    this.checkExceptions = null;
    this.threshold = 20;
    this.interval = 60000;
    this.exceptionCounter = {};
};
util.inherits(Scale, events.EventEmitter);

// Initial connect function.  Connect to AWS using both plata and the AWS SDK.
// Get all of the tables, and make the in-memory throughput exceeded exception counter.
// Start the counter on an interval, emit the 'connected' event.
Scale.prototype.connect = function(config){
    var self = this;
    aws.connect(config.aws);
    this.dynamo = aws.dynamo;
    this.listTables().then(function(tables){
        self.tables = tables.map(function(table){
            self.exceptionCounter[table] = {
                'read': [],
                'write': []
            };
            return table;
        });
        self.startCounter();
        self.emit('connect');
    });
};

// NOT REALLY IMPLEMENTED
// Take a table name, the operation (Read or Write), and a value to scale its
// throughout put (ex. 1.1, 1.2, up to 2.0)
// @todo (lucas) Automatically handle stepping up by the doubling algorithm.
// Don't resolve promise until the scaling has completed.
Scale.prototype.scaleThroughput = function(tableName, reads, writes){
    // return this.dynamo.describeTable({'TableName': tableName}).then(function(data){
        return this.dyanmo.updateTable({
            'TableName': tableName,
            'ProvisionedThroughput': {
                'WriteCapacityUnits': writes,
                'ReadCapacityUnits': reads
            }
        });
    // });
};

// List all of the dynamo tables associated with these AWS credentials
Scale.prototype.listTables = function(){
    return this.dynamo.listTables().then(function(res){
        return res.TableNames;
    });
};

Scale.prototype.startCounter = function(){
    var self = this;
    function inform(exceptionSeries){
        var windowLength = 5,
            set = new gauss.TimeSeries(exceptionSeries),
            ema = set.values().ema(windowLength);

        ema.reverse();
        if(ema[0] > ema[ema.length - 1]){
            return Math.ceil(Math.max.apply(ema, ema));
        }
        return 0;
    }

    this.checkExceptions = setTimeout(function(){
        // this.exceptionCounter coutnains the number of throughput exceeded
        // exceptions for a table and operation per second.

        // If the expoenential moving average is trending upwards,
        // we need to scale up to the highest point.

        // To handle scaling down, we'll need to look at how low we are
        // below are provisioned throughput we are... which is difficult
        // because the default Cloudwatch metrics are delayed 5 minutes minimim
        // and we can only scale down once per day.
        // IMO this is actually a whole other feature, figuring out the min
        // required throughput per 24 hours.  We could find the minimum
        // for today last week and scale down to that value at that time.?

        // Back to scaling up.
        // Figure out if we need to do something, by how much and then do it.
        // Also trim the data window for exception counts.
        when.all(Object.keys(self.exceptionCounter).map(function(tableName){
            var scaleUpReads = inform(self.exceptionCounter[tableName].reads),
                scaleUpWrites = inform(self.exceptionCounter[tableName].writes),
                res = {
                    'table': tableName,
                    'reads': scaleUpReads,
                    'writes': scaleUpWrites
                };

            if(scaleUpReads > 0 || scaleUpWrites > 0){
                return self.dynamo.describeTable({'TableName': tableName}).then(function(data){
                    var provisioned = data.Table.ProvidiedThroughput,
                        newReads = scaleUpReads + provisioned.ReadCapacityUnits,
                        newWrites = scaleUpWrites + provisioned.WriteCapacityUnits;
                    return self.scaleThroughput(tableName, newReads, newWrites).then(function(){
                        return res;
                    });
                });
            }
            return res;
        }), function(results){
            results.forEach(function(res){
                console.log('Table ' + res.table + ' increase reads ' +
                    res.reads + ' and writes ' + res.writes);
            });
            self.startCounter();
        });
    }, this.interval);
};

// This will get hit by SNS when there is a throughput exceeded exception for a table.
Scale.prototype.addException = function(tableName, operation, timestamp){
    this.exceptionCounter[tableName][operation].push(timestamp);
};

// CloudWatch stuff, if we decide to use it.
Scale.prototype.checkThroughput = function(){
    var self = this,
        throughputStats = {};

    return when.all(this.tables.map(function(table){
        return self.doThroughputCheck(table).then(function(data){
            throughputStats[table] = data;
        });
    })).then(function(){
        return throughputStats;
    });
};

// Grab normalized read and write throughput stats from cloudwatch for a table
// over the previous 10 hours.
Scale.prototype.doThroughputCheck = function(table){
    var throughputStats = {
            'write': [],
            'read': []
        },
        self = this,
        timeRangeMinutes = 600,
        start = new Date(new Date().getTime() - 1000*60*timeRangeMinutes),
        end = new Date(),
        tableName,
        writeCapacity,
        readCapacity;

    return self.dynamo.describeTable({'TableName': table}).then(function(res){
        writeCapacity = res.data.Table.ProvisionedThroughput.WriteCapacityUnits;
        readCapacity = res.data.Table.ProvisionedThroughput.ReadCapacityUnits;
        return when.all(['read', 'write'].map(function(operation){
            return self.getThroughputStatistics(table, operation, start, end).then(function(stats){
                if (stats.datapoints !== null){
                    stats.datapoints.member.map(function(pnt){
                        throughputStats[operation].push({
                            'timestamp': pnt.timestamp,
                            'value': (pnt.sum/300)/writeCapacity
                        });
                    });
                }
            });
        }));
    }).then(function(){
        return throughputStats;
    });
};


// Get throughput stats from cloudwatch for a table.
Scale.prototype.getThroughputStatistics = function(tableName, operation, startTime, endTime){
    var d = when.defer(),
        metrics = {
            'write': 'ConsumedWriteCapacityUnits',
            'read': 'ConsumedReadCapacityUnits'
        };
    aws.cloudWatch.getMetricStatistics('AWS/DynamoDB', metrics[operation],
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
    'create': create
};
