/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.consensus.membership;

import com.neo4j.causalclustering.identity.MemberId;
import com.neo4j.causalclustering.messaging.EndOfStreamException;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.neo4j.io.fs.ReadableChannel;
import org.neo4j.io.fs.WritableChannel;

/**
 * Format:
 * ┌────────────────────────────────────────────┐
 * │ memberCount                        4 bytes │
 * │ member 0   ┌──────────────────────────────┐│
 * │            │mostSignificantBits    8 bytes││
 * │            │leastSignificantBits   8 bytes││
 * │            └──────────────────────────────┘│
 * │ ...                                        │
 * │ member n   ┌──────────────────────────────┐│
 * │            │mostSignificantBits    8 bytes││
 * │            │leastSignificantBits   8 bytes││
 * │            └──────────────────────────────┘│
 * └────────────────────────────────────────────┘
 */
public class MemberIdSetSerializer
{
    private MemberIdSetSerializer()
    {
    }

    public static void marshal( MemberIdSet memberSet, WritableChannel channel ) throws IOException
    {
        Set<MemberId> members = memberSet.getMembers();
        channel.putInt( members.size() );

        MemberId.Marshal memberIdMarshal = new MemberId.Marshal();

        for ( MemberId member : members )
        {
            memberIdMarshal.marshal( member, channel );
        }
    }

    public static MemberIdSet unmarshal( ReadableChannel channel ) throws IOException, EndOfStreamException
    {
        HashSet<MemberId> members = new HashSet<>();
        int memberCount = channel.getInt();

        MemberId.Marshal memberIdMarshal = new MemberId.Marshal();

        for ( int i = 0; i < memberCount; i++ )
        {
            members.add( memberIdMarshal.unmarshal( channel ) );
        }

        return new MemberIdSet( members );
    }
}
