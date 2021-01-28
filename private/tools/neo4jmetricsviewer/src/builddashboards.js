/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
const fs = require('fs');

var path = '/etc/grafana/provisioning/dashboards/metrics-dashboard.json'
var metricsDir = '/src/metrics/'
var rawdata = fs.readFileSync(path);
var json = JSON.parse(rawdata)
var panelstr = JSON.stringify(json.panels[0]);
json.panels = [];
var files = fs.readdirSync(metricsDir)
var index = 0;
for (var file in files) {
    var panel = JSON.parse(panelstr);
    panel.title = files[file];
    panel.targets[0].target = files[file];
    if (index % 2 == 0)
        panel.gridPos.x = 12;
    panel.id = ++index;
    json.panels.push(panel);
}
fs.writeFileSync(path, JSON.stringify(json))