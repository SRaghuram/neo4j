/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.state.storage;

import java.io.IOException;

public interface StateStorage<STATE>
{
    STATE getInitialState();

    void persistStoreData( STATE state ) throws IOException;
}
