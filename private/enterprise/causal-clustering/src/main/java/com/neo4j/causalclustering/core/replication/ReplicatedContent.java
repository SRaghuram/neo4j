/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.replication;

import com.neo4j.causalclustering.messaging.marshalling.ReplicatedContentHandler;

import java.io.IOException;
import java.util.OptionalLong;

/**
 * Marker interface for types that can be replicated around.
 */
public interface ReplicatedContent
{
    default OptionalLong size()
    {
        return OptionalLong.empty();
    }

    void dispatch( ReplicatedContentHandler contentHandler ) throws IOException;
}
