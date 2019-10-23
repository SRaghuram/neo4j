/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.consensus.log.debug;

import com.neo4j.causalclustering.core.consensus.log.segmented.CoreLogPruningStrategy;
import com.neo4j.causalclustering.core.consensus.log.segmented.CoreLogPruningStrategyFactory;
import com.neo4j.causalclustering.core.consensus.log.segmented.SegmentedRaftLog;
import com.neo4j.causalclustering.core.replication.ReplicatedContent;
import com.neo4j.causalclustering.core.state.machines.tx.ReplicatedTransaction;
import com.neo4j.causalclustering.core.state.machines.tx.ReplicatedTransactionFactory;
import com.neo4j.causalclustering.messaging.marshalling.CoreReplicatedContentMarshalV2;

import java.io.File;
import java.io.IOException;

import org.neo4j.configuration.Config;
import org.neo4j.internal.helpers.Args;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryReader;
import org.neo4j.kernel.impl.transaction.log.entry.VersionAwareLogEntryReader;
import org.neo4j.logging.LogProvider;
import org.neo4j.test.scheduler.ThreadPoolJobScheduler;
import org.neo4j.time.Clocks;

import static com.neo4j.causalclustering.core.CausalClusteringSettings.raft_log_pruning_strategy;
import static com.neo4j.causalclustering.core.CausalClusteringSettings.raft_log_reader_pool_size;
import static com.neo4j.causalclustering.core.CausalClusteringSettings.raft_log_rotation_size;
import static com.neo4j.causalclustering.core.consensus.log.RaftLogHelper.readLogEntry;
import static org.neo4j.logging.NullLogProvider.getInstance;

public class ReplayRaftLog
{
    private ReplayRaftLog()
    {
    }

    public static void main( String[] args ) throws IOException
    {
        Args arg = Args.parse( args );

        String from = arg.get( "from" );
        System.out.println( "From is " + from );
        String to = arg.get( "to" );
        System.out.println( "To is " + to );
        File logDirectory = new File( from );
        System.out.println( "logDirectory = " + logDirectory );
        Config config = Config.defaults();

        try ( DefaultFileSystemAbstraction fileSystem = new DefaultFileSystemAbstraction() )
        {
            LogProvider logProvider = getInstance();
            CoreLogPruningStrategy pruningStrategy =
                    new CoreLogPruningStrategyFactory( config.get( raft_log_pruning_strategy ), logProvider ).newInstance();

            SegmentedRaftLog log = new SegmentedRaftLog( fileSystem, logDirectory, config.get( raft_log_rotation_size ),
                    ignored -> new CoreReplicatedContentMarshalV2(), logProvider, config.get( raft_log_reader_pool_size ),
                    Clocks.systemClock(), new ThreadPoolJobScheduler(), pruningStrategy );

            long totalCommittedEntries = log.appendIndex(); // Not really, but we need to have a way to pass in the commit index
            LogEntryReader reader = new VersionAwareLogEntryReader();
            for ( int i = 0; i <= totalCommittedEntries; i++ )
            {
                ReplicatedContent content = readLogEntry( log, i ).content();
                if ( content instanceof ReplicatedTransaction )
                {
                    ReplicatedTransaction tx = (ReplicatedTransaction) content;
                    ReplicatedTransactionFactory.extractTransactionRepresentation( tx, new byte[0], reader ).accept( element ->
                    {
                        System.out.println( element );
                        return false;
                    } );
                }
            }
        }
    }
}
