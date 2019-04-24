/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.state.storage;

import java.io.IOException;

public interface SimpleStorage<T> extends StateStorage<T>
{
    T readState() throws IOException;

    @Override
    default T getInitialState()
    {
        return null;
    }
}
