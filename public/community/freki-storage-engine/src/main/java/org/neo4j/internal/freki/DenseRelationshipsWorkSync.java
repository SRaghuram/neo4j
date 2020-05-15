/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.internal.freki;

import java.util.ArrayList;
import java.util.List;

import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.util.concurrent.AsyncApply;
import org.neo4j.util.concurrent.Work;
import org.neo4j.util.concurrent.WorkSync;

public class DenseRelationshipsWorkSync
{
    private WorkSync<DenseRelationshipStore,DenseRelationshipsWork> workSync;

    public DenseRelationshipsWorkSync( DenseRelationshipStore store )
    {
        workSync = new WorkSync<>( store );
    }

    public Batch newBatch()
    {
        return new Batch();
    }

    public class Batch
    {
        private final List<FrekiCommand.DenseNode> commands = new ArrayList<>();

        public void add( FrekiCommand.DenseNode node )
        {
            commands.add( node );
        }

        AsyncApply applyAsync( PageCacheTracer cacheTracer )
        {
            return workSync.applyAsync( new DenseRelationshipsWork( commands, cacheTracer ) );
        }
    }

    private static class DenseRelationshipsWork implements Work<DenseRelationshipStore,DenseRelationshipsWork>
    {
        private final List<FrekiCommand.DenseNode> commands;
        private PageCacheTracer tracer;

        DenseRelationshipsWork( List<FrekiCommand.DenseNode> commands, PageCacheTracer tracer )
        {
            this.commands = commands;
            this.tracer = tracer;
        }

        @Override
        public DenseRelationshipsWork combine( DenseRelationshipsWork work )
        {
            commands.addAll( work.commands );
            return this;
        }

        @Override
        public void apply( DenseRelationshipStore store ) throws Exception
        {
            try ( PageCursorTracer cursorTracer = tracer.createPageCursorTracer( "Dense relationships" ) )
            {
                try ( DenseRelationshipStore.Updater updater = store.newUpdater( cursorTracer ) )
                {
                    for ( FrekiCommand.DenseNode node : commands )
                    {
                        node.relationshipUpdates.forEachKeyValue( ( type, typedRelationships ) ->
                        {
                            typedRelationships.deleted.forEach( relationship ->
                                    updater.deleteRelationship( relationship.internalId, node.nodeId, type, relationship.otherNodeId, relationship.outgoing ) );
                            typedRelationships.inserted.forEach( relationship -> updater.insertRelationship( relationship.internalId, node.nodeId, type,
                                    relationship.otherNodeId, relationship.outgoing, relationship.propertyUpdates, u -> u.after ) );
                        } );
                    }
                }
            }
        }
    }
}
