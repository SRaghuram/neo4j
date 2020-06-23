/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.akka.marshal;

import com.neo4j.causalclustering.discovery.akka.database.state.DatabaseToMember;
import com.neo4j.causalclustering.identity.MemberId;

import java.io.IOException;

import org.neo4j.io.fs.ReadableChannel;
import org.neo4j.io.fs.WritableChannel;
import org.neo4j.io.marshal.EndOfStreamException;
import org.neo4j.io.marshal.SafeChannelMarshal;

public class DatabaseToMemberMarshal extends SafeChannelMarshal<DatabaseToMember>
{
    public static final DatabaseToMemberMarshal INSTANCE = new DatabaseToMemberMarshal();

    private DatabaseToMemberMarshal()
    {
    }

    @Override
    protected DatabaseToMember unmarshal0( ReadableChannel channel ) throws IOException, EndOfStreamException
    {
        var databaseId = DatabaseIdWithoutNameMarshal.INSTANCE.unmarshal( channel );
        var memberId = MemberId.Marshal.INSTANCE.unmarshal( channel );
        return new DatabaseToMember( databaseId, memberId );
    }

    @Override
    public void marshal( DatabaseToMember databaseToMember, WritableChannel channel ) throws IOException
    {
        DatabaseIdWithoutNameMarshal.INSTANCE.marshal( databaseToMember.databaseId(), channel );
        MemberId.Marshal.INSTANCE.marshal( databaseToMember.memberId(), channel );
    }
}
