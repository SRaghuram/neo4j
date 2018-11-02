/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.discovery;

import java.util.Set;

public class TopologyDifference
{
    private Set<Difference> added;
    private Set<Difference> removed;

    TopologyDifference( Set<Difference> added, Set<Difference> removed )
    {
        this.added = added;
        this.removed = removed;
    }

    Set<Difference> added()
    {
        return added;
    }

    Set<Difference> removed()
    {
        return removed;
    }

    public boolean hasChanges()
    {
        return added.size() > 0 || removed.size() > 0;
    }

    @Override
    public String toString()
    {
        return String.format( "{added=%s, removed=%s}", added, removed );
    }
}
