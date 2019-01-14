/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.com;

import java.nio.channels.ReadableByteChannel;

public interface MadeUpWriter
{
    void write( ReadableByteChannel data );
}
