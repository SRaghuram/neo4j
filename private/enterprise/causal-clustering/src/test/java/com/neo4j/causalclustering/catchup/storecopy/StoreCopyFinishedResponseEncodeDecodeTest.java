/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.catchup.storecopy;

import com.neo4j.causalclustering.catchup.storecopy.StoreCopyFinishedResponse.Status;
import com.neo4j.causalclustering.catchup.v3.storecopy.StoreCopyFinishedResponseDecoder;
import com.neo4j.causalclustering.catchup.v3.storecopy.StoreCopyFinishedResponseEncoder;
import io.netty.channel.ChannelHandler;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;

class StoreCopyFinishedResponseEncodeDecodeTest
{
    @ParameterizedTest
    @EnumSource( Versions.class )
    void shouldEncodeAndDecodePullRequestMessage( Versions version )
    {
        // given
        EmbeddedChannel channel = new EmbeddedChannel( version.encoder(), version.decoder() );
        StoreCopyFinishedResponse sent = new StoreCopyFinishedResponse( Status.E_STORE_ID_MISMATCH, 10 );

        // when
        channel.writeOutbound( sent );
        Object message = channel.readOutbound();
        channel.writeInbound( message );

        // then
        StoreCopyFinishedResponse received = channel.readInbound();
        assertNotSame( sent, received );
        assertEquals( sent, received );
        assertEquals( -1, received.lastCheckpointedTx() );
    }

    @ParameterizedTest
    @EnumSource( Versions.class )
    void shouldEncodeAndDecodePullRequestMessageSuccess( Versions version )
    {
        // given
        EmbeddedChannel channel = new EmbeddedChannel( version.encoder(), version.decoder() );
        StoreCopyFinishedResponse sent = new StoreCopyFinishedResponse( Status.SUCCESS, 10 );

        // when
        channel.writeOutbound( sent );
        Object message = channel.readOutbound();
        channel.writeInbound( message );

        // then
        StoreCopyFinishedResponse received = channel.readInbound();
        assertNotSame( sent, received );
        if ( version == Versions.V3 )
        {
            assertEquals( sent, received );
            assertEquals( 10, received.lastCheckpointedTx() );
        }
        else
        {
            assertEquals( sent.status(), received.status() );
            assertEquals( -1, received.lastCheckpointedTx() );
        }
    }

    private enum Versions
    {
        V3
                {
                    @Override
                    ChannelHandler encoder()
                    {
                        return new StoreCopyFinishedResponseEncoder();
                    }

                    @Override
                    ChannelHandler decoder()
                    {
                        return new StoreCopyFinishedResponseDecoder();
                    }
                };

        abstract ChannelHandler encoder();

        abstract ChannelHandler decoder();
    }

}
