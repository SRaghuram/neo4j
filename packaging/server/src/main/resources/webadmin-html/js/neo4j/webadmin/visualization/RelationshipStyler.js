(function() {
  /*
  Copyright (c) 2002-2011 "Neo Technology,"
  Network Engine for Objects in Lund AB [http://neotechnology.com]

  This file is part of Neo4j.

  Neo4j is free software: you can redistribute it and/or modify
  it under the terms of the GNU Affero General Public License as
  published by the Free Software Foundation, either version 3 of the
  License, or (at your option) any later version.

  This program is distributed in the hope that it will be useful,
  but WITHOUT ANY WARRANTY; without even the implied warranty of
  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
  GNU Affero General Public License for more details.

  You should have received a copy of the GNU Affero General Public License
  along with this program. If not, see <http://www.gnu.org/licenses/>.
  */  define([], function() {
    var RelationshipStyler;
    return RelationshipStyler = (function() {
      function RelationshipStyler() {}
      RelationshipStyler.prototype.defaultBetweenExploredStyle = {
        edgeStyle: {
          color: "rgba(0, 0, 0, 1)",
          width: 1
        },
        labelStyle: {
          color: "white",
          font: "10px Helvetica"
        }
      };
      RelationshipStyler.prototype.defaultToUnknownStyle = {
        edgeStyle: {
          color: "rgba(0, 0, 0, 0.2)",
          width: 1
        },
        labelStyle: {
          color: "rgba(0, 0, 0, 0.4)",
          font: "10px Helvetica"
        }
      };
      RelationshipStyler.prototype.defaultToGroupStyle = {
        edgeStyle: {
          color: "rgba(0, 0, 0, 0.5)",
          width: 1
        },
        labelStyle: {
          color: "rgba(0, 0, 0, 0.4)",
          font: "10px Helvetica"
        }
      };
      RelationshipStyler.prototype.getStyleFor = function(visualRelationship) {
        var dstType, rel, srcType, types;
        srcType = visualRelationship.source.data.type;
        dstType = visualRelationship.target.data.type;
        if (srcType === "explored-node" && dstType === "explored-node") {
          types = (function() {
            var _i, _len, _ref, _results;
            _ref = visualRelationship.data.relationships;
            _results = [];
            for (_i = 0, _len = _ref.length; _i < _len; _i++) {
              rel = _ref[_i];
              _results.push(rel.getType());
            }
            return _results;
          })();
          return {
            edgeStyle: this.defaultBetweenExploredStyle.edgeStyle,
            labelStyle: this.defaultBetweenExploredStyle.labelStyle,
            labelText: types.join(", ")
          };
        }
        if (srcType === "unexplored-node" || dstType === "unexplored-node") {
          types = (function() {
            var _i, _len, _ref, _results;
            _ref = visualRelationship.data.relationships;
            _results = [];
            for (_i = 0, _len = _ref.length; _i < _len; _i++) {
              rel = _ref[_i];
              _results.push(rel.getType());
            }
            return _results;
          })();
          return {
            edgeStyle: this.defaultToUnknownStyle.edgeStyle,
            labelStyle: this.defaultToUnknownStyle.labelStyle,
            labelText: types.join(", ")
          };
        }
        return {
          edgeStyle: this.defaultToGroupStyle.edgeStyle,
          labelStyle: this.defaultToGroupStyle.labelStyle,
          labelText: "hello, world!"
        };
      };
      return RelationshipStyler;
    })();
  });
}).call(this);
