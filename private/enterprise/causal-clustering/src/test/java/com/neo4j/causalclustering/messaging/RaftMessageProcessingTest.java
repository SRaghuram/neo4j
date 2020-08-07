/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.messaging;

import com.neo4j.causalclustering.core.consensus.RaftMessages;
import com.neo4j.causalclustering.core.consensus.ReplicatedInteger;
import com.neo4j.causalclustering.core.consensus.log.RaftLogEntry;
import com.neo4j.causalclustering.identity.IdFactory;
import com.neo4j.causalclustering.identity.MemberId;
import com.neo4j.causalclustering.messaging.marshalling.ContentTypeProtocol;
import com.neo4j.causalclustering.messaging.marshalling.RaftMessageDecoder;
import com.neo4j.causalclustering.messaging.marshalling.RaftMessageEncoder;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class RaftMessageProcessingTest
{
    private EmbeddedChannel channel;

    @BeforeEach
    void setup()
    {
        channel = new EmbeddedChannel( new RaftMessageEncoder(), new RaftMessageDecoder( new ContentTypeProtocol() ) );
    }

    @Test
    void shouldEncodeAndDecodeVoteRequest()
    {
        // given
        MemberId member = IdFactory.randomMemberId();
        RaftMessages.Vote.Request request = new RaftMessages.Vote.Request( member, 1, member, 1, 1 );

        // when
        channel.writeOutbound( request );
        Object message = channel.readOutbound();
        channel.writeInbound( message );

        // then
        Assertions.assertEquals( request, channel.readInbound() );
    }

    @Test
    void shouldEncodeAndDecodeVoteResponse()
    {
        // given
        MemberId member = IdFactory.randomMemberId();
        RaftMessages.Vote.Response response = new RaftMessages.Vote.Response( member, 1, true );

        // when
        channel.writeOutbound( response );
        Object message = channel.readOutbound();
        channel.writeInbound( message );

        // then
        Assertions.assertEquals( response, channel.readInbound() );
    }

    @Test
    void shouldEncodeAndDecodeAppendEntriesRequest()
    {
        // given
        MemberId member = IdFactory.randomMemberId();
        RaftLogEntry logEntry = new RaftLogEntry( 1, ReplicatedInteger.valueOf( 1 ) );
        RaftMessages.AppendEntries.Request request =
                new RaftMessages.AppendEntries.Request(
                        member, 1, 1, 99, new RaftLogEntry[]{logEntry}, 1 );

        // when
        channel.writeOutbound( request );
        Object message = channel.readOutbound();
        channel.writeInbound( message );

        // then
        Assertions.assertEquals( request, channel.readInbound() );
    }

    @Test
    void shouldEncodeAndDecodeAppendEntriesResponse()
    {
        // given
        MemberId member = IdFactory.randomMemberId();
        RaftMessages.AppendEntries.Response response =
                new RaftMessages.AppendEntries.Response( member, 1, false, -1, 0 );

        // when
        channel.writeOutbound( response );
        Object message = channel.readOutbound();
        channel.writeInbound( message );

        // then
        Assertions.assertEquals( response, channel.readInbound() );
    }

    @Test
    void shouldEncodeAndDecodeNewEntryRequest()
    {
        // given
        MemberId member = IdFactory.randomMemberId();
        RaftMessages.NewEntry.Request request =
                new RaftMessages.NewEntry.Request( member, ReplicatedInteger.valueOf( 12 ) );

        // when
        channel.writeOutbound( request );
        Object message = channel.readOutbound();
        channel.writeInbound( message );

        // then
        Assertions.assertEquals( request, channel.readInbound() );
    }
}
