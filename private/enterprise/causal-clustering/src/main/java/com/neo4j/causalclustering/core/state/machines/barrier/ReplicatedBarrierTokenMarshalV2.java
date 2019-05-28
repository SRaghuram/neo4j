/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.state.machines.barrier;

import java.io.IOException;

import com.neo4j.causalclustering.identity.MemberId;
import com.neo4j.causalclustering.messaging.EndOfStreamException;
import com.neo4j.causalclustering.messaging.marshalling.DatabaseIdMarshal;

import org.neo4j.io.fs.ReadableChannel;
import org.neo4j.io.fs.WritableChannel;
import org.neo4j.kernel.database.DatabaseId;

public class ReplicatedBarrierTokenMarshalV2
{
    private ReplicatedBarrierTokenMarshalV2()
    {
    }

    public static void marshal( ReplicatedBarrierTokenRequest tokenRequest, WritableChannel channel ) throws IOException
    {
        DatabaseIdMarshal.INSTANCE.marshal( tokenRequest.databaseId(), channel );
        channel.putInt( tokenRequest.id() );
        new MemberId.Marshal().marshal( tokenRequest.owner(), channel );
    }

    public static ReplicatedBarrierTokenRequest unmarshal( ReadableChannel channel ) throws IOException, EndOfStreamException
    {
        DatabaseId databaseId = DatabaseIdMarshal.INSTANCE.unmarshal( channel );
        int candidateId = channel.getInt();
        MemberId owner = new MemberId.Marshal().unmarshal( channel );
        return new ReplicatedBarrierTokenRequest( owner, candidateId, databaseId );
    }
}
