/*jshint esversion: 6 */
/*
	Simple webserver for serving Grafana Simple JSON plugin (https://github.com/grafana/simple-json-datasource)
	With inspiration from https://github.com/michaeldmoore/CSVServer
	Reads CSV metrics files from Neo4J, converts to json and responds on web requests
	Currently only handles one data metric (leftmost) per CSV file
*/
const server = require('fastify')({
	logger: false
})

const csv = require('csvtojson');
const fs = require('fs');

const metricsDir = 'metrics';

server.get('/', function (request, reply) {
	reply.send(); //Grafana test connection, return 200
});


server.post('/search', (request, reply) => {
	reply.send(fs.readdirSync(metricsDir)); //return a list of available metrics (available files)
});

server.post('/query', function (request, reply) {
	//return metrics based on request
	var result = [];
	request.body.targets.forEach(function (target) {
		target.dateRange = request.body.range;
		target.maxDataPoints = request.body.maxDataPoints;
		var p = new Promise(function (resolve, reject) {
			query(target).then(function (data) {
				resolve(data);
			});
		});
		p.then(function (val) {
			result.push(val);
			if (result.length == request.body.targets.length)
				reply.send(result);
		}).catch(function (reason) {
			server.log.error(reason);
		});
	});
});

const cache = new Map()
async function query(target) {
	var filename = metricsDir + '/' + target.target;
	var json;
	if (cache.has(filename)) {
		json = cache.get(filename);
	}
	else {
		json = await csv().fromFile(filename, { headers: true, trim: true });
		cache.set(filename, json);
	}
	return parseJson(target, json);
}

function parseJson(target, json) {
	var result = {};
	result.target = Object.keys(json[0])[1];
	result.datapoints = [];
	var pointsWithinRange = [];
	var from = new Date(target.dateRange.from).getTime();
	var to = new Date(target.dateRange.to).getTime();
	json.forEach(function (row) {
		var array = Object.keys(row).map(function (key) { return row[key]; });
		var pointTime = array[0] * 1000; //timestamp -> unix timestamp in ms
		if (pointTime >= from && pointTime <= to) { //within requested range
			var dataPoint = []; //format: [data,timestamp]
			dataPoint[0] = Number(array[1]);
			dataPoint[1] = pointTime;
			pointsWithinRange.push(dataPoint);
		}
	});
	var everyNpoint = Math.ceil(pointsWithinRange.length / target.maxDataPoints);
	for (var point in pointsWithinRange) { // return less than maxDataPoints
		if (point % everyNpoint == 0) {
			result.datapoints.push(pointsWithinRange[point]);
		}
	}
	return result;
}

server.listen(4000, err => {
	if (err) {
		console.log.error(err);
		process.exit(1);
	}
});