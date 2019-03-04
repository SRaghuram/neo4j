/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.scenarios;

import com.neo4j.causalclustering.common.Cluster;
import com.neo4j.causalclustering.common.DataCreator;
import com.neo4j.causalclustering.core.CoreClusterMember;
import com.neo4j.causalclustering.read_replica.ReadReplica;
import com.neo4j.test.causalclustering.ClusterConfig;
import com.neo4j.test.causalclustering.ClusterExtension;
import com.neo4j.test.causalclustering.ClusterFactory;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.File;
import java.io.IOException;

import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryWriter;
import org.neo4j.kernel.impl.transaction.log.files.LogFiles;
import org.neo4j.kernel.impl.transaction.log.files.LogFilesBuilder;
import org.neo4j.kernel.lifecycle.Lifespan;
import org.neo4j.test.extension.DefaultFileSystemExtension;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.pagecache.PageCacheExtension;

import static com.neo4j.causalclustering.common.Cluster.dataMatchesEventually;
import static java.util.Collections.singletonList;

/**
 * Recovery scenarios where the transaction log was only partially written.
 */
@PageCacheExtension
@ClusterExtension
@ExtendWith( DefaultFileSystemExtension.class )
class TransactionLogRecoveryIT
{
    @Inject
    private PageCache pageCache;

    @Inject
    private ClusterFactory clusterFactory;

    @Inject
    private DefaultFileSystemAbstraction fs;

    private Cluster cluster;

    @BeforeAll
    void setup() throws Exception
    {
        cluster = clusterFactory.createCluster( ClusterConfig.clusterConfig().withNumberOfCoreMembers( 3 ).withNumberOfReadReplicas( 3 ) );
        cluster.start();
    }

    @Test
    void coreShouldStartAfterPartialTransactionWriteCrash() throws Exception
    {
        // given: a fully synced cluster with some data
        dataMatchesEventually( DataCreator.createEmptyNodes( cluster, 10 ), cluster.coreMembers() );

        // when: shutting down a core
        CoreClusterMember core = cluster.getCoreMemberById( 0 );
        core.shutdown();

        // and making sure there will be something new to pull
        CoreClusterMember lastWrites = DataCreator.createEmptyNodes( cluster, 10 );

        // and writing a partial tx
        writePartialTx( core.databaseDirectory() );

        // then: we should still be able to start
        core.start();

        // and become fully synced again
        dataMatchesEventually( lastWrites, singletonList( core ) );
    }

    @Test
    void coreShouldStartWithSeedHavingPartialTransactionWriteCrash() throws Exception
    {
        // given: a fully synced cluster with some data
        dataMatchesEventually( DataCreator.createEmptyNodes( cluster, 10 ), cluster.coreMembers() );

        // when: shutting down a core
        CoreClusterMember core = cluster.getCoreMemberById( 0 );
        core.shutdown();

        // and making sure there will be something new to pull
        CoreClusterMember lastWrites = DataCreator.createEmptyNodes( cluster, 10 );

        // and writing a partial tx
        writePartialTx( core.databaseDirectory() );

        // and deleting the cluster state, making sure a snapshot is required during startup
        // effectively a seeding scenario -- representing the use of the unbind command on a crashed store
        fs.deleteRecursively( core.clusterStateDirectory() );

        // then: we should still be able to start
        core.start();

        // and become fully synced again
        dataMatchesEventually( lastWrites, singletonList( core ) );
    }

    @Test
    void readReplicaShouldStartAfterPartialTransactionWriteCrash() throws Exception
    {
        // given: a fully synced cluster with some data
        dataMatchesEventually( DataCreator.createEmptyNodes( cluster, 10 ), cluster.readReplicas() );

        // when: shutting down a read replica
        ReadReplica readReplica = cluster.getReadReplicaById( 0 );
        readReplica.shutdown();

        // and making sure there will be something new to pull
        CoreClusterMember lastWrites = DataCreator.createEmptyNodes( cluster, 10 );
        dataMatchesEventually( lastWrites, cluster.coreMembers() );

        // and writing a partial tx
        writePartialTx( readReplica.databaseDirectory() );

        // then: we should still be able to start
        readReplica.start();

        // and become fully synced again
        dataMatchesEventually( lastWrites, singletonList( readReplica ) );
    }

    private void writePartialTx( File storeDir ) throws IOException
    {
        LogFiles logFiles = LogFilesBuilder.activeFilesBuilder( DatabaseLayout.of( storeDir ), fs, pageCache ).build();
        try ( Lifespan ignored = new Lifespan( logFiles ) )
        {
            LogEntryWriter writer = new LogEntryWriter( logFiles.getLogFile().getWriter() );
            writer.writeStartEntry( 0, 0, 0x123456789ABCDEFL, logFiles.getLogFileInformation().getLastEntryId() + 1, new byte[]{0} );
        }
    }
}
