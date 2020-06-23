/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.consensus.membership;

import com.neo4j.causalclustering.identity.MemberId;
import com.neo4j.causalclustering.messaging.BoundedNetworkWritableChannel;
import com.neo4j.causalclustering.messaging.NetworkReadableChannel;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.UUID;

import org.neo4j.io.marshal.EndOfStreamException;

class MemberIdMarshalTest
{
    @Test
    void shouldSerializeAndDeserialize() throws Exception
    {
        // given
        MemberId.Marshal marshal = new MemberId.Marshal();

        final MemberId member = new MemberId( UUID.randomUUID() );

        // when
        ByteBuf buffer = Unpooled.buffer( 1_000 );
        marshal.marshal( member, new BoundedNetworkWritableChannel( buffer ) );
        final MemberId recovered = marshal.unmarshal( new NetworkReadableChannel( buffer ) );

        // then
        Assertions.assertEquals( member, recovered );
    }

    @Test
    void shouldThrowExceptionForHalfWrittenInstance() throws Exception
    {
        // given
        // a CoreMember and a ByteBuffer to write it to
        MemberId.Marshal marshal = new MemberId.Marshal();
        final MemberId aRealMember = new MemberId( UUID.randomUUID() );

        ByteBuf buffer = Unpooled.buffer( 1000 );

        // and the CoreMember is serialized but for 5 bytes at the end
        marshal.marshal( aRealMember, new BoundedNetworkWritableChannel( buffer ) );
        ByteBuf bufferWithMissingBytes = buffer.copy( 0, buffer.writerIndex() - 5 );

        // when
        try
        {
            marshal.unmarshal( new NetworkReadableChannel( bufferWithMissingBytes ) );
            Assertions.fail( "Should have thrown exception" );
        }
        catch ( EndOfStreamException e )
        {
            // expected
        }
    }
}
