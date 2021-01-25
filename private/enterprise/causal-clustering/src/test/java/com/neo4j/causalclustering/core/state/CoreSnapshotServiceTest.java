/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.state;

import com.neo4j.causalclustering.core.CoreState;
import com.neo4j.causalclustering.core.consensus.RaftMachine;
import com.neo4j.causalclustering.core.consensus.log.RaftLog;
import com.neo4j.causalclustering.core.consensus.state.ExposedRaftState;
import com.neo4j.dbms.DatabaseStartAborter;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import org.neo4j.dbms.database.DatabaseStartAbortedException;
import org.neo4j.kernel.database.TestDatabaseIdRepository;
import org.neo4j.logging.internal.NullLogService;
import org.neo4j.time.FakeClock;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class CoreSnapshotServiceTest
{
    @Test
    void shouldEventuallyAbort()
    {
        // given
        var raftMachine = mock( RaftMachine.class );
        var raftState = mock( ExposedRaftState.class );
        when( raftState.appendIndex() ).thenReturn( -1L );
        when( raftMachine.state() ).thenReturn( raftState );

        var databaseId = TestDatabaseIdRepository.randomNamedDatabaseId();

        var snapshotService = new CoreSnapshotService( mock( CommandApplicationProcess.class ), mock( RaftLog.class ), mock( CoreState.class ),
                raftMachine, databaseId, NullLogService.getInstance(), new FakeClock() );

        var databaseStartAborter = mock( DatabaseStartAborter.class );
        when( databaseStartAborter.shouldAbort( databaseId ) ).thenReturn( false ).thenReturn( true );

        // when/then
        assertThrows( DatabaseStartAbortedException.class, () -> snapshotService.awaitState( databaseStartAborter, Duration.ofMillis( 10 ) ) );
        verify( raftState, times( 2 ) ).appendIndex();
    }
}
