const fs = require('fs');

var path = '/etc/grafana/provisioning/dashboards/metrics-dashboard.json'
var metricsDir = '/src/metrics/'
var rawdata = fs.readFileSync(path);
var json = JSON.parse(rawdata)
var panelstr = JSON.stringify(json.panels[0]);
json.panels = [];
var files = fs.readdirSync(metricsDir)
var index = 0;
for ( var file in files )
{
    var panel = JSON.parse(panelstr);
    panel.title = files[file];
    panel.id = ++index;
    panel.targets[0].target = files[file];
    json.panels.push(panel);
}
fs.writeFileSync(path, JSON.stringify(json))