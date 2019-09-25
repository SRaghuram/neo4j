/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package com.neo4j.fabric.stream.summary;

import java.util.concurrent.atomic.AtomicInteger;

import org.neo4j.graphdb.QueryStatistics;

public class MergedQueryStatistics implements QueryStatistics
{
    private final AtomicInteger nodesCreated = new AtomicInteger( 0 );
    private final AtomicInteger nodesDeleted = new AtomicInteger( 0 );
    private final AtomicInteger relationshipsCreated = new AtomicInteger( 0 );
    private final AtomicInteger relationshipsDeleted = new AtomicInteger( 0 );
    private final AtomicInteger propertiesSet = new AtomicInteger( 0 );
    private final AtomicInteger labelsAdded = new AtomicInteger( 0 );
    private final AtomicInteger labelsRemoved = new AtomicInteger( 0 );
    private final AtomicInteger indexesAdded = new AtomicInteger( 0 );
    private final AtomicInteger indexesRemoved = new AtomicInteger( 0 );
    private final AtomicInteger constraintsAdded = new AtomicInteger( 0 );
    private final AtomicInteger constraintsRemoved = new AtomicInteger( 0 );
    private final AtomicInteger systemUpdates = new AtomicInteger( 0 );
    private boolean containsUpdates = false;
    private boolean containsSystemUpdates = false;

    public void add( QueryStatistics delta )
    {
        nodesCreated.addAndGet( delta.getNodesCreated() );
        nodesDeleted.addAndGet( delta.getNodesDeleted() );
        relationshipsCreated.addAndGet( delta.getRelationshipsCreated() );
        relationshipsDeleted.addAndGet( delta.getRelationshipsDeleted() );
        propertiesSet.addAndGet( delta.getPropertiesSet() );
        labelsAdded.addAndGet( delta.getLabelsAdded() );
        labelsRemoved.addAndGet( delta.getLabelsRemoved() );
        indexesAdded.addAndGet( delta.getIndexesAdded() );
        indexesRemoved.addAndGet( delta.getIndexesRemoved() );
        constraintsAdded.addAndGet( delta.getConstraintsAdded() );
        constraintsRemoved.addAndGet( delta.getConstraintsRemoved() );
        systemUpdates.addAndGet( delta.getSystemUpdates() );
        if ( delta.containsUpdates() )
        {
            containsUpdates = true;
        }
        if ( delta.containsUpdates() )
        {
            containsSystemUpdates = true;
        }
    }

    @Override
    public int getNodesCreated()
    {
        return nodesCreated.get();
    }

    @Override
    public int getNodesDeleted()
    {
        return nodesDeleted.get();
    }

    @Override
    public int getRelationshipsCreated()
    {
        return relationshipsCreated.get();
    }

    @Override
    public int getRelationshipsDeleted()
    {
        return relationshipsDeleted.get();
    }

    @Override
    public int getPropertiesSet()
    {
        return propertiesSet.get();
    }

    @Override
    public int getLabelsAdded()
    {
        return labelsAdded.get();
    }

    @Override
    public int getLabelsRemoved()
    {
        return labelsRemoved.get();
    }

    @Override
    public int getIndexesAdded()
    {
        return indexesAdded.get();
    }

    @Override
    public int getIndexesRemoved()
    {
        return indexesRemoved.get();
    }

    @Override
    public int getConstraintsAdded()
    {
        return constraintsAdded.get();
    }

    @Override
    public int getConstraintsRemoved()
    {
        return constraintsRemoved.get();
    }

    @Override
    public int getSystemUpdates()
    {
        return systemUpdates.get();
    }

    @Override
    public boolean containsUpdates()
    {
        return containsUpdates;
    }

    @Override
    public boolean containsSystemUpdates()
    {
        return containsSystemUpdates;
    }
}
