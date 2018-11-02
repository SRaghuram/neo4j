/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.com;

import java.io.Serializable;

class StartupData implements Serializable
{
    private static final long serialVersionUID = 3570271945897559074L;

    final int port;
    final long creationTime;
    final long storeId;
    final byte applicationProtocolVersion;
    final byte internalProtocolVersion;
    final int chunkSize;

    StartupData( long creationTime, long storeId, byte internalProtocolVersion, byte applicationProtocolVersion,
            int chunkSize, int port )
    {
        this.creationTime = creationTime;
        this.storeId = storeId;
        this.internalProtocolVersion = internalProtocolVersion;
        this.applicationProtocolVersion = applicationProtocolVersion;
        this.chunkSize = chunkSize;
        this.port = port;
    }
}
