/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.state;

import com.neo4j.causalclustering.core.CoreState;
import com.neo4j.causalclustering.core.consensus.RaftMachine;
import com.neo4j.causalclustering.core.consensus.log.RaftLog;
import com.neo4j.causalclustering.core.state.snapshot.CoreSnapshot;
import com.neo4j.dbms.DatabaseStartAborter;

import java.io.IOException;

import org.neo4j.dbms.database.DatabaseStartAbortedException;
import org.neo4j.kernel.database.DatabaseId;

public class CoreSnapshotService
{
    private static final String OPERATION_NAME = "snapshot request";

    private final CommandApplicationProcess applicationProcess;
    private final CoreState coreState;
    private final RaftLog raftLog;
    private final RaftMachine raftMachine;
    private final DatabaseId databaseId;

    public CoreSnapshotService( CommandApplicationProcess applicationProcess, RaftLog raftLog, CoreState coreState,
            RaftMachine raftMachine, DatabaseId databaseId )
    {
        this.applicationProcess = applicationProcess;
        this.coreState = coreState;
        this.raftLog = raftLog;
        this.raftMachine = raftMachine;
        this.databaseId = databaseId;
    }

    public synchronized CoreSnapshot snapshot() throws Exception
    {
        applicationProcess.pauseApplier( OPERATION_NAME );
        try
        {
            long lastApplied = applicationProcess.lastApplied();

            long prevTerm = raftLog.readEntryTerm( lastApplied );
            CoreSnapshot coreSnapshot = new CoreSnapshot( lastApplied, prevTerm );

            coreState.augmentSnapshot( coreSnapshot );
            coreSnapshot.add( CoreStateFiles.RAFT_CORE_STATE, raftMachine.coreState() );

            return coreSnapshot;
        }
        finally
        {
            applicationProcess.resumeApplier( OPERATION_NAME );
        }
    }

    public synchronized void installSnapshot( CoreSnapshot coreSnapshot ) throws IOException
    {
        long snapshotPrevIndex = coreSnapshot.prevIndex();
        raftLog.skip( snapshotPrevIndex, coreSnapshot.prevTerm() );
        raftMachine.installCoreState( coreSnapshot.get( CoreStateFiles.RAFT_CORE_STATE ) );

        coreState.installSnapshot( coreSnapshot );
        coreState.flush( snapshotPrevIndex );

        applicationProcess.installSnapshot( coreSnapshot );
        notifyAll();
    }

    public synchronized void awaitState( DatabaseStartAborter startAborter ) throws InterruptedException, DatabaseStartAbortedException
    {
        while ( raftMachine.state().appendIndex() < 0 )
        {
            if ( startAborter.shouldAbort( databaseId ) )
            {
                throw new DatabaseStartAbortedException( databaseId );
            }
            wait();
        }
    }
}
