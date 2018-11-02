/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.core.state.snapshot;

import org.neo4j.causalclustering.helper.TimeoutStrategy;

class NoTimeout implements TimeoutStrategy.Timeout
{
    private int increments;

    @Override
    public long getMillis()
    {
        return 0;
    }

    @Override
    public void increment()
    {
        increments++;
    }

    public int currentCount()
    {
        return increments;
    }
}
