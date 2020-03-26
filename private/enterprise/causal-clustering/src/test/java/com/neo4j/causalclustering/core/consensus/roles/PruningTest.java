/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.consensus.roles;

import com.neo4j.causalclustering.core.consensus.RaftMessages;
import com.neo4j.causalclustering.core.consensus.log.InMemoryRaftLog;
import com.neo4j.causalclustering.core.consensus.outcome.Outcome;
import com.neo4j.causalclustering.core.consensus.outcome.PruneLogCommand;
import com.neo4j.causalclustering.core.consensus.state.RaftState;
import com.neo4j.causalclustering.identity.MemberId;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;

import org.neo4j.logging.Log;
import org.neo4j.logging.NullLogProvider;

import static com.neo4j.causalclustering.core.consensus.state.RaftStateBuilder.builder;
import static com.neo4j.causalclustering.identity.RaftTestMember.member;
import static org.assertj.core.api.Assertions.assertThat;

@RunWith( Parameterized.class )
public class PruningTest
{
    @Parameterized.Parameters( name = "{0}" )
    public static Collection<Object[]> data()
    {
        return Arrays.asList( new Object[][]{
                {Role.FOLLOWER}, {Role.LEADER}, {Role.CANDIDATE}
        } );
    }

    @Parameterized.Parameter
    public Role role;

    private MemberId myself = member( 0 );

    @Test
    public void shouldGeneratePruneCommandsOnRequest() throws Exception
    {
        // given
        InMemoryRaftLog raftLog = new InMemoryRaftLog();
        RaftState state = builder()
                .myself( myself )
                .entryLog( raftLog )
                .build();

        // when
        RaftMessages.PruneRequest pruneRequest = new RaftMessages.PruneRequest( 1000 );
        Outcome outcome = role.handler.handle( pruneRequest, state, log() );

        // then
        assertThat( outcome.getLogCommands() ).contains( new PruneLogCommand( 1000 ) );
    }

    private Log log()
    {
        return NullLogProvider.getInstance().getLog( getClass() );
    }
}
