/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.upstream;

import java.util.Iterator;
import java.util.NoSuchElementException;

import org.neo4j.logging.NullLogProvider;

public class NoOpUpstreamDatabaseStrategiesLoader extends UpstreamDatabaseStrategiesLoader
{
    public NoOpUpstreamDatabaseStrategiesLoader()
    {
        super( null, null, null, NullLogProvider.getInstance() );
    }

    @Override
    public Iterator<UpstreamDatabaseSelectionStrategy> iterator()
    {
        return new Iterator<>()
        {
            @Override
            public boolean hasNext()
            {
                return false;
            }

            @Override
            public UpstreamDatabaseSelectionStrategy next()
            {
                throw new NoSuchElementException();
            }
        };
    }
}
