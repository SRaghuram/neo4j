/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.ha.correctness;

import org.neo4j.helpers.collection.Iterables;

public class InstanceCrashedAction implements ClusterAction
{
    private final String instanceUri;

    public InstanceCrashedAction( String instanceUri )
    {
        this.instanceUri = instanceUri;
    }

    @Override
    public Iterable<ClusterAction> perform( ClusterState state ) throws Exception
    {
        state.instance( instanceUri ).crash();
        return Iterables.empty();
    }

    @Override
    public String toString()
    {
        return "[CRASH " + instanceUri + "]";
    }

    @Override
    public boolean equals( Object o )
    {
        if ( this == o )
        {
            return true;
        }
        if ( o == null || getClass() != o.getClass() )
        {
            return false;
        }

        return instanceUri.equals( ((InstanceCrashedAction)o).instanceUri );
    }

    @Override
    public int hashCode()
    {
        return instanceUri.hashCode();
    }
}
