/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.com;

public class Protocol320 extends Protocol310
{
    public Protocol320( int chunkSize, byte applicationProtocolVersion, byte internalProtocolVersion )
    {
        super( chunkSize, applicationProtocolVersion, internalProtocolVersion );
    }
}
