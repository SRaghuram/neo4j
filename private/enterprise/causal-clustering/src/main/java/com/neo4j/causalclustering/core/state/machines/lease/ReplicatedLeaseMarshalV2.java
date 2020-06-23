/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.state.machines.lease;

import com.neo4j.causalclustering.discovery.akka.marshal.DatabaseIdWithoutNameMarshal;
import com.neo4j.causalclustering.identity.MemberId;

import java.io.IOException;

import org.neo4j.io.fs.ReadableChannel;
import org.neo4j.io.fs.WritableChannel;
import org.neo4j.io.marshal.EndOfStreamException;
import org.neo4j.kernel.database.DatabaseId;

public class ReplicatedLeaseMarshalV2
{
    private ReplicatedLeaseMarshalV2()
    {
    }

    public static void marshal( ReplicatedLeaseRequest leaseRequest, WritableChannel channel ) throws IOException
    {
        DatabaseIdWithoutNameMarshal.INSTANCE.marshal( leaseRequest.databaseId(), channel );
        channel.putInt( leaseRequest.id() );
        new MemberId.Marshal().marshal( leaseRequest.owner(), channel );
    }

    public static ReplicatedLeaseRequest unmarshal( ReadableChannel channel ) throws IOException, EndOfStreamException
    {
        DatabaseId databaseId = DatabaseIdWithoutNameMarshal.INSTANCE.unmarshal( channel );
        int leaseId = channel.getInt();
        MemberId owner = new MemberId.Marshal().unmarshal( channel );
        return new ReplicatedLeaseRequest( owner, leaseId, databaseId );
    }
}
