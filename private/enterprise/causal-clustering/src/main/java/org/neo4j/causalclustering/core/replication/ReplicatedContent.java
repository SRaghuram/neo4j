/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.core.replication;

import java.io.IOException;
import java.util.OptionalLong;

import org.neo4j.causalclustering.messaging.marshalling.ReplicatedContentHandler;

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
