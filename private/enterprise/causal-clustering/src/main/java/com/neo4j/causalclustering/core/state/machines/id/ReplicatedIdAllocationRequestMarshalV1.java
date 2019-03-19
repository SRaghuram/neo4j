/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.state.machines.id;

import com.neo4j.causalclustering.identity.MemberId;
import com.neo4j.causalclustering.messaging.EndOfStreamException;

import java.io.IOException;

import org.neo4j.internal.id.IdType;
import org.neo4j.io.fs.ReadableChannel;
import org.neo4j.io.fs.WritableChannel;

public class ReplicatedIdAllocationRequestMarshalV1
{
    private ReplicatedIdAllocationRequestMarshalV1()
    {
    }

    public static void marshal( ReplicatedIdAllocationRequest idRangeRequest, WritableChannel channel ) throws IOException
    {
        new MemberId.Marshal().marshal( idRangeRequest.owner(), channel );
        channel.putInt( idRangeRequest.idType().ordinal() );
        channel.putLong( idRangeRequest.idRangeStart() );
        channel.putInt( idRangeRequest.idRangeLength() );
    }

    public static ReplicatedIdAllocationRequest unmarshal( ReadableChannel channel, String databaseName ) throws IOException, EndOfStreamException
    {
        MemberId owner = new MemberId.Marshal().unmarshal( channel );
        IdType idType = IdType.values()[ channel.getInt() ];
        long idRangeStart = channel.getLong();
        int idRangeLength = channel.getInt();
        return new ReplicatedIdAllocationRequest( owner, idType, idRangeStart, idRangeLength, databaseName );
    }
}
