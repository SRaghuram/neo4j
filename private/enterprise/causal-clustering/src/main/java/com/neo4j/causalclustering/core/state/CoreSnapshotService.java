/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.state;

import com.neo4j.causalclustering.core.consensus.RaftMachine;
import com.neo4j.causalclustering.core.consensus.log.RaftLog;
import com.neo4j.causalclustering.core.state.snapshot.CoreSnapshot;

import java.io.IOException;
import java.util.Map;

import org.neo4j.kernel.database.DatabaseId;

import static java.util.Comparator.comparingLong;

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

    public synchronized CoreSnapshot snapshot( DatabaseId databaseId ) throws Exception
    {
        applicationProcess.pauseApplier( OPERATION_NAME );
        try
        {
            long lastApplied = applicationProcess.lastApplied();

            long prevTerm = raftLog.readEntryTerm( lastApplied );
            CoreSnapshot coreSnapshot = new CoreSnapshot( lastApplied, prevTerm );

            coreStateRepository.augmentSnapshot( databaseId, coreSnapshot );
            coreSnapshot.add( CoreStateFiles.RAFT_CORE_STATE, raftMachine.coreState() );

            return coreSnapshot;
        }
        finally
        {
            applicationProcess.resumeApplier( OPERATION_NAME );
        }
    }

    public synchronized void installSnapshots( Map<DatabaseId,CoreSnapshot> coreSnapshots ) throws IOException
    {
        CoreSnapshot earliestSnapshot = coreSnapshots.values().stream().min( comparingLong( CoreSnapshot::prevIndex ) ).orElseThrow();

        long snapshotPrevIndex = earliestSnapshot.prevIndex();
        raftLog.skip( snapshotPrevIndex, earliestSnapshot.prevTerm() );
        raftMachine.installCoreState( earliestSnapshot.get( CoreStateFiles.RAFT_CORE_STATE ) );

        coreSnapshots.forEach( coreStateRepository::installSnapshotForDatabase );
        coreStateRepository.installSnapshotForRaftGroup( earliestSnapshot );
        coreStateRepository.flush( snapshotPrevIndex );

        applicationProcess.installSnapshot( earliestSnapshot );
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
