/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.messaging;

import com.neo4j.causalclustering.core.consensus.RaftMessages;
import com.neo4j.causalclustering.core.consensus.ReplicatedInteger;
import com.neo4j.causalclustering.core.consensus.log.RaftLogEntry;
import com.neo4j.causalclustering.identity.MemberId;
import com.neo4j.causalclustering.messaging.marshalling.v2.ContentTypeProtocol;
import com.neo4j.causalclustering.messaging.marshalling.v2.decoding.RaftMessageDecoder;
import com.neo4j.causalclustering.messaging.marshalling.v2.encoding.RaftMessageEncoder;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.UUID;

import static org.junit.Assert.assertEquals;

@RunWith( MockitoJUnitRunner.class )
public class RaftMessageProcessingTest
{
    private EmbeddedChannel channel;

    @Before
    public void setup()
    {
        channel = new EmbeddedChannel( new RaftMessageEncoder(), new RaftMessageDecoder( new ContentTypeProtocol() ) );
    }

    @Test
    public void shouldEncodeAndDecodeVoteRequest()
    {
        // given
        MemberId member = new MemberId( UUID.randomUUID() );
        RaftMessages.Vote.Request request = new RaftMessages.Vote.Request( member, 1, member, 1, 1 );

        // when
        channel.writeOutbound( request );
        Object message = channel.readOutbound();
        channel.writeInbound( message );

        // then
        assertEquals( request, channel.readInbound() );
    }

    @Test
    public void shouldEncodeAndDecodeVoteResponse()
    {
        // given
        MemberId member = new MemberId( UUID.randomUUID() );
        RaftMessages.Vote.Response response = new RaftMessages.Vote.Response( member, 1, true );

        // when
        channel.writeOutbound( response );
        Object message = channel.readOutbound();
        channel.writeInbound( message );

        // then
        assertEquals( response, channel.readInbound() );
    }

    @Test
    public void shouldEncodeAndDecodeAppendEntriesRequest()
    {
        // given
        MemberId member = new MemberId( UUID.randomUUID() );
        RaftLogEntry logEntry = new RaftLogEntry( 1, ReplicatedInteger.valueOf( 1 ) );
        RaftMessages.AppendEntries.Request request =
                new RaftMessages.AppendEntries.Request(
                        member, 1, 1, 99, new RaftLogEntry[] { logEntry }, 1 );

        // when
        channel.writeOutbound( request );
        Object message = channel.readOutbound();
        channel.writeInbound( message );

        // then
        assertEquals( request, channel.readInbound() );
    }

    @Test
    public void shouldEncodeAndDecodeAppendEntriesResponse()
    {
        // given
        MemberId member = new MemberId( UUID.randomUUID() );
        RaftMessages.AppendEntries.Response response =
                new RaftMessages.AppendEntries.Response( member, 1, false, -1, 0 );

        // when
        channel.writeOutbound( response );
        Object message = channel.readOutbound();
        channel.writeInbound( message );

        // then
        assertEquals( response, channel.readInbound() );
    }

    @Test
    public void shouldEncodeAndDecodeNewEntryRequest()
    {
        // given
        MemberId member = new MemberId( UUID.randomUUID() );
        RaftMessages.NewEntry.Request request =
                new RaftMessages.NewEntry.Request( member, ReplicatedInteger.valueOf( 12 ) );

        // when
        channel.writeOutbound( request );
        Object message = channel.readOutbound();
        channel.writeInbound( message );

        // then
        assertEquals( request, channel.readInbound() );
    }
}
