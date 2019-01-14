/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.core.state.machines.locks;

import java.io.IOException;

import org.neo4j.causalclustering.identity.MemberId;
import org.neo4j.causalclustering.messaging.EndOfStreamException;
import org.neo4j.storageengine.api.ReadableChannel;
import org.neo4j.storageengine.api.WritableChannel;

public class ReplicatedLockTokenMarshalV1
{
    private ReplicatedLockTokenMarshalV1()
    {
    }

    public static void marshal( ReplicatedLockTokenRequest tokenRequest, WritableChannel channel ) throws IOException
    {
        channel.putInt( tokenRequest.id() );
        new MemberId.Marshal().marshal( tokenRequest.owner(), channel );
    }

    public static ReplicatedLockTokenRequest unmarshal( ReadableChannel channel, String databaseName ) throws IOException, EndOfStreamException
    {
        int candidateId = channel.getInt();
        MemberId owner = new MemberId.Marshal().unmarshal( channel );
        return new ReplicatedLockTokenRequest( owner, candidateId, databaseName );
    }
}
