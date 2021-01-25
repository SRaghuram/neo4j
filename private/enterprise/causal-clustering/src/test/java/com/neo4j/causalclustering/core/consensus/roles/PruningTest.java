/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.consensus.roles;

import com.neo4j.causalclustering.core.consensus.RaftMessages;
import com.neo4j.causalclustering.core.consensus.log.InMemoryRaftLog;
import com.neo4j.causalclustering.core.consensus.outcome.Outcome;
import com.neo4j.causalclustering.core.consensus.outcome.PruneLogCommand;
import com.neo4j.causalclustering.core.consensus.state.RaftState;
import com.neo4j.causalclustering.identity.RaftMemberId;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import org.neo4j.logging.Log;
import org.neo4j.logging.NullLogProvider;

import static com.neo4j.causalclustering.core.consensus.state.RaftMessageHandlingContextBuilder.contextWithState;
import static com.neo4j.causalclustering.core.consensus.state.RaftStateBuilder.builder;
import static com.neo4j.causalclustering.identity.RaftTestMember.raftMember;
import static org.assertj.core.api.Assertions.assertThat;

class PruningTest
{
    private RaftMemberId myself = raftMember( 0 );

    @ParameterizedTest
    @EnumSource( Role.class )
    void shouldGeneratePruneCommandsOnRequest( Role role ) throws Exception
    {
        // given
        InMemoryRaftLog raftLog = new InMemoryRaftLog();
        RaftState state = builder()
                .myself( myself )
                .entryLog( raftLog )
                .build();

        // when
        RaftMessages.PruneRequest pruneRequest = new RaftMessages.PruneRequest( 1000 );
        Outcome outcome = role.handler.handle( pruneRequest, contextWithState( state ), log() );

        // then
        assertThat( outcome.getLogCommands() ).contains( new PruneLogCommand( 1000 ) );
    }

    private Log log()
    {
        return NullLogProvider.getInstance().getLog( getClass() );
    }
}
