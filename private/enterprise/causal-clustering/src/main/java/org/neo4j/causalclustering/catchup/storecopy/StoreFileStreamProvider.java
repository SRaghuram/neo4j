/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.catchup.storecopy;

import java.io.IOException;

public interface StoreFileStreamProvider
{
    StoreFileStream acquire( String destination, int requiredAlignment ) throws IOException;
}
