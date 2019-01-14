/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.core.state;

import java.io.IOException;

import org.neo4j.causalclustering.core.consensus.RaftMachine;
import org.neo4j.causalclustering.core.consensus.log.RaftLog;
import org.neo4j.causalclustering.core.state.snapshot.CoreSnapshot;

public class CoreSnapshotService
{
    private static final String OPERATION_NAME = "snapshot request";

    private final CommandApplicationProcess applicationProcess;
    private final CoreStateRepository coreStateRepository;
    private final RaftLog raftLog;
    private final RaftMachine raftMachine;

    public CoreSnapshotService( CommandApplicationProcess applicationProcess, CoreStateRepository coreStateRepository, RaftLog raftLog,
            RaftMachine raftMachine )
    {
        this.applicationProcess = applicationProcess;
        this.coreStateRepository = coreStateRepository;
        this.raftLog = raftLog;
        this.raftMachine = raftMachine;
    }

    public synchronized CoreSnapshot snapshot() throws Exception
    {
        applicationProcess.pauseApplier( OPERATION_NAME );
        try
        {
            long lastApplied = applicationProcess.lastApplied();

            long prevTerm = raftLog.readEntryTerm( lastApplied );
            CoreSnapshot coreSnapshot = new CoreSnapshot( lastApplied, prevTerm );

            coreStateRepository.augmentSnapshot( coreSnapshot );
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

        coreStateRepository.installSnapshot( coreSnapshot );
        raftMachine.installCoreState( coreSnapshot.get( CoreStateFiles.RAFT_CORE_STATE ) );
        coreStateRepository.flush( snapshotPrevIndex );

        applicationProcess.installSnapshot( coreSnapshot );
        notifyAll();
    }

    synchronized void awaitState() throws InterruptedException
    {
        while ( raftMachine.state().appendIndex() < 0 )
        {
            wait();
        }
    }
}
