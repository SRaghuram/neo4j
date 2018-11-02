/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.core.consensus;

import java.io.IOException;

import org.neo4j.causalclustering.core.replication.ReplicatedContent;
import org.neo4j.causalclustering.messaging.marshalling.ReplicatedContentHandler;

/**
 * When a new leader is elected, it replicates one entry of this type to mark the start of its reign.
 * By listening for content of this type, we can partition content by leader reign.
 */
public class NewLeaderBarrier implements ReplicatedContent
{
    @Override
    public String toString()
    {
        return "NewLeaderBarrier";
    }

    @Override
    public int hashCode()
    {
        return 1;
    }

    @Override
    public boolean equals( Object obj )
    {
        return obj instanceof NewLeaderBarrier;
    }

    @Override
    public void handle( ReplicatedContentHandler contentHandler ) throws IOException
    {
        contentHandler.handle( this );
    }
}
