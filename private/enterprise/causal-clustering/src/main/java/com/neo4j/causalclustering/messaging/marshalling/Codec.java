/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.messaging.marshalling;

import com.neo4j.causalclustering.messaging.EndOfStreamException;
import io.netty.buffer.ByteBuf;

import java.io.IOException;
import java.util.List;

public interface Codec<CONTENT>
{
    void encode( CONTENT type, List<Object> output ) throws IOException;

    ContentBuilder<CONTENT> decode( ByteBuf byteBuf ) throws IOException, EndOfStreamException;
}
