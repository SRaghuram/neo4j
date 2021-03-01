/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.consensus.membership;

import com.neo4j.causalclustering.identity.IdFactory;
import com.neo4j.causalclustering.identity.RaftMemberId;
import com.neo4j.causalclustering.messaging.BoundedNetworkWritableChannel;
import com.neo4j.causalclustering.messaging.NetworkReadableChannel;
import com.neo4j.causalclustering.test_helpers.BaseMarshalTest;
import io.netty.buffer.Unpooled;
import org.junit.jupiter.api.Test;

import java.util.Collection;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.neo4j.io.marshal.ChannelMarshal;
import org.neo4j.io.marshal.EndOfStreamException;

import static org.junit.jupiter.api.Assertions.assertThrows;

class RaftMemberIdMarshalTest implements BaseMarshalTest<RaftMemberId>
{
    @Override
    public ChannelMarshal<RaftMemberId> marshal()
    {
        return RaftMemberId.Marshal.INSTANCE;
    }

    @Override
    public Collection<RaftMemberId> originals()
    {
        return Stream.generate( IdFactory::randomRaftMemberId )
                     .limit( 5 )
                     .collect( Collectors.toList() );
    }

    @Test
    void shouldThrowExceptionForHalfWrittenInstance() throws Exception
    {
        // given
        // a RaftMemberId and a ByteBuffer to write it to
        var aRealMember = IdFactory.randomRaftMemberId();
        var buffer = Unpooled.buffer( 1000 );
        var marshal = marshal();

        // and the RaftMemberId is serialized but for 5 bytes at the end
        marshal.marshal( aRealMember, new BoundedNetworkWritableChannel( buffer ) );
        var bufferWithMissingBytes = buffer.copy( 0, buffer.writerIndex() - 5 );

        // when
        assertThrows( EndOfStreamException.class, () -> marshal.unmarshal( new NetworkReadableChannel( bufferWithMissingBytes ) ) );
    }
}
