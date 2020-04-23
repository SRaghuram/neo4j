/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
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
    private boolean containsUpdates;
    private boolean containsSystemUpdates;

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
        if ( delta.containsSystemUpdates() )
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

    @Override
    public String toString()
    {
        var builder = new StringBuilder();

        if ( containsSystemUpdates )
        {
            includeIfNonZero( builder, "System updates: ", systemUpdates.get() );
        }
        else
        {
            includeIfNonZero( builder, "Nodes created: ", nodesCreated.get() );
            includeIfNonZero( builder, "Relationships created: ", relationshipsCreated.get() );
            includeIfNonZero( builder, "Properties set: ", propertiesSet.get() );
            includeIfNonZero( builder, "Nodes deleted: ", nodesDeleted.get() );
            includeIfNonZero( builder, "Relationships deleted: ", relationshipsDeleted.get() );
            includeIfNonZero( builder, "Labels added: ", labelsAdded.get() );
            includeIfNonZero( builder, "Labels removed: ", labelsRemoved.get() );
            includeIfNonZero( builder, "Indexes added: ", indexesAdded.get() );
            includeIfNonZero( builder, "Indexes removed: ", indexesRemoved.get() );
            includeIfNonZero( builder, "Constraints added: ", constraintsAdded.get() );
            includeIfNonZero( builder, "Constraints removed: ", constraintsRemoved.get() );
        }
        var result = builder.toString();

        if ( result.isEmpty() )
        {
            return "<Nothing happened>";
        }
        else
        {
            return result;
        }
    }

    private void includeIfNonZero( StringBuilder builder, String message, long count )
    {
        if ( count > 0 )
        {
            builder.append( message ).append( count ).append( "\n" );
        }
    }
}
