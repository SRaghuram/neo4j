/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.messaging.marshalling;

import com.neo4j.causalclustering.core.consensus.RaftMessages;
import com.neo4j.causalclustering.messaging.marshalling.v2.SupportedMessagesV2;
import com.neo4j.causalclustering.messaging.marshalling.v3.SupportedMessagesV3;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

class SupportedMessagesTest
{
    private static List<Boolean> mockV2Messages( SupportedMessages handler )
    {
        return List.of(
                handler.handle( mock( RaftMessages.Vote.Request.class ) ),
                handler.handle( mock( RaftMessages.Vote.Response.class ) ),
                handler.handle( mock( RaftMessages.PreVote.Request.class ) ),
                handler.handle( mock( RaftMessages.PreVote.Response.class ) ),
                handler.handle( mock( RaftMessages.AppendEntries.Request.class ) ),
                handler.handle( mock( RaftMessages.AppendEntries.Response.class ) ),
                handler.handle( mock( RaftMessages.Heartbeat.class ) ),
                handler.handle( mock( RaftMessages.HeartbeatResponse.class ) ),
                handler.handle( mock( RaftMessages.LogCompactionInfo.class ) ),
                handler.handle( mock( RaftMessages.Timeout.Election.class ) ),
                handler.handle( mock( RaftMessages.Timeout.Heartbeat.class ) ),
                handler.handle( mock( RaftMessages.NewEntry.Request.class ) ),
                handler.handle( mock( RaftMessages.NewEntry.BatchRequest.class ) ),
                handler.handle( mock( RaftMessages.PruneRequest.class ) )
        );
    }

    private static List<Boolean> mockV3Messages( SupportedMessages handler )
    {
        return List.of(
               handler.handle( mock( RaftMessages.LeadershipTransfer.Proposal.class ) ),
               handler.handle( mock( RaftMessages.LeadershipTransfer.Request.class ) ),
               handler.handle( mock( RaftMessages.LeadershipTransfer.Rejection.class ) )
        );
    }

    private static List<Boolean> mockV4Messages( SupportedMessages handler )
    {
        return List.of( handler.handle( mock( RaftMessages.StatusResponse.class ) ) );
    }

    private static List<Boolean> allMessages( SupportedMessages handler )
    {
        return Stream.concat( mockV2Messages( handler ).stream(),
                              Stream.concat( mockV3Messages( handler ).stream(),
                                             mockV4Messages( handler ).stream() ) ).collect( Collectors.toList() );
    }

    @Test
    void supportAllHandlerShouldHandleAllMessages()
    {
        var supported = SupportedMessages.SUPPORT_ALL;
        assertThat( allMessages( supported ) ).allMatch( supports -> supports );
    }

    @Test
    void v2MessageHandlerShouldHandleV2Messages()
    {
        var supported = new SupportedMessagesV2();
        assertThat( mockV2Messages( supported ) ).allMatch( supports -> supports );
    }

    @Test
    void v2MessageHandlerShouldNotHandleLaterMessages()
    {
        var supported = new SupportedMessagesV2();
        assertThat( mockV3Messages( supported ) ).allMatch( supports -> !supports );
        assertThat( mockV4Messages( supported ) ).allMatch( supports -> !supports );
    }

    @Test
    void v3MessageHandlerShouldHandleV3Messages()
    {
        var supported = new SupportedMessagesV3();
        assertThat( mockV2Messages( supported ) ).allMatch( supports -> supports );
        assertThat( mockV3Messages( supported ) ).allMatch( supports -> supports );
    }

    @Test
    void v3MessageHandlerShouldNotHandlerLaterMessages()
    {
        var supported = new SupportedMessagesV3();
        assertThat( mockV4Messages( supported ) ).allMatch( supports -> !supports );
    }
}
