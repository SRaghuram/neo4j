###
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
###

define(
  [], 
  () ->
  
    class RelationshipStyler

      defaultBetweenExploredStyle :
        edgeStyle :
          color : "rgba(0, 0, 0, 1)"
          width : 1
        labelStyle : 
          color : "white"
          font  : "10px Helvetica"

      defaultToUnknownStyle : 
        edgeStyle :
          color : "rgba(0, 0, 0, 0.2)"
          width : 1
        labelStyle : 
          color : "rgba(0, 0, 0, 0.4)"
          font  : "10px Helvetica"

      defaultToGroupStyle : 
        edgeStyle : 
          color : "rgba(0, 0, 0, 0.5)"
          width : 1
        labelStyle : 
          color : "rgba(0, 0, 0, 0.4)"
          font  : "10px Helvetica"

      getStyleFor : (visualRelationship) ->
        srcType = visualRelationship.source.data.type
        dstType = visualRelationship.target.data.type
        if srcType is "explored-node" and dstType is "explored-node"
          types = for rel in visualRelationship.data.relationships
            rel.getType()

          return { edgeStyle : @defaultBetweenExploredStyle.edgeStyle, labelStyle : @defaultBetweenExploredStyle.labelStyle , labelText : types.join(", ") }

        if srcType is "unexplored-node" or dstType is "unexplored-node"
          types = for rel in visualRelationship.data.relationships
            rel.getType()
          return { edgeStyle : @defaultToUnknownStyle.edgeStyle, labelStyle : @defaultToUnknownStyle.labelStyle, labelText : types.join(", ") }
        
        return { edgeStyle : @defaultToGroupStyle.edgeStyle, labelStyle : @defaultToGroupStyle.labelStyle, labelText : "hello, world!" }

)
