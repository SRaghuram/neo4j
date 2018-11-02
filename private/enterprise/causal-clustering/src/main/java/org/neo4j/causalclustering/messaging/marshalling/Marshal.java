/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.messaging.marshalling;

import java.io.IOException;

import org.neo4j.storageengine.api.WritableChannel;

public interface Marshal
{
    /**
     * Writes all content to the channel
     *
     * @param channel to where data is written.
     */
    void marshal( WritableChannel channel ) throws IOException;
}
