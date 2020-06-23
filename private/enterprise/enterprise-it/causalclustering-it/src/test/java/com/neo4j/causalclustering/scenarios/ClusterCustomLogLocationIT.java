/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.scenarios;

import com.neo4j.causalclustering.common.Cluster;
import com.neo4j.causalclustering.core.CoreClusterMember;
import com.neo4j.causalclustering.read_replica.ReadReplica;
import com.neo4j.causalclustering.readreplica.CatchupProcessManager;
import com.neo4j.test.causalclustering.ClusterExtension;
import com.neo4j.test.causalclustering.ClusterFactory;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Collection;

import org.neo4j.common.DependencyResolver;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.kernel.impl.transaction.log.files.LogFiles;
import org.neo4j.storageengine.api.StorageEngineFactory;
import org.neo4j.test.extension.Inject;

import static com.neo4j.test.causalclustering.ClusterConfig.clusterConfig;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.kernel.impl.transaction.log.files.LogFilesBuilder.logFilesBasedOnlyBuilder;

@ClusterExtension
class ClusterCustomLogLocationIT
{
    @Inject
    private ClusterFactory clusterFactory;

    private Cluster cluster;

    @BeforeAll
    void beforeAll() throws Exception
    {
        cluster = clusterFactory.createCluster( clusterConfig().withNumberOfCoreMembers( 3 ).withNumberOfReadReplicas( 2 ) );
        cluster.start();
    }

    @Test
    void clusterWithCustomTransactionLogLocation() throws Exception
    {
        for ( int i = 0; i < 10; i++ )
        {
            cluster.coreTx( ( db, tx ) ->
            {
                tx.createNode();
                tx.commit();
            } );
        }

        Collection<CoreClusterMember> coreClusterMembers = cluster.coreMembers();
        for ( CoreClusterMember coreClusterMember : coreClusterMembers )
        {
            DependencyResolver dependencyResolver = coreClusterMember.defaultDatabase().getDependencyResolver();
            LogFiles logFiles = dependencyResolver.resolveDependency( LogFiles.class );
            assertEquals( logFiles.logFilesDirectory().getName(), coreClusterMember.defaultDatabase().databaseName() );
            assertTrue( logFiles.hasAnyEntries( 0 ) );

            logFileInStoreDirectoryDoesNotExist( coreClusterMember.databaseLayout(), dependencyResolver );
        }

        Collection<ReadReplica> readReplicas = cluster.readReplicas();
        for ( ReadReplica readReplica : readReplicas )
        {
            CatchupProcessManager catchupProcessManager = readReplica.resolveDependency( DEFAULT_DATABASE_NAME, CatchupProcessManager.class );
            catchupProcessManager.getCatchupProcess().upToDateFuture().get();
            DependencyResolver dependencyResolver = readReplica.defaultDatabase().getDependencyResolver();
            LogFiles logFiles = dependencyResolver.resolveDependency( LogFiles.class );
            assertEquals( logFiles.logFilesDirectory().getName(), readReplica.defaultDatabase().databaseName() );
            assertTrue( logFiles.hasAnyEntries( 0 ) );

            logFileInStoreDirectoryDoesNotExist( readReplica.databaseLayout(), dependencyResolver );
        }
    }

    private static void logFileInStoreDirectoryDoesNotExist( DatabaseLayout databaseLayout, DependencyResolver dependencyResolver ) throws IOException
    {
        FileSystemAbstraction fileSystem = dependencyResolver.resolveDependency( FileSystemAbstraction.class );
        LogFiles storeLogFiles = logFilesBasedOnlyBuilder( databaseLayout.databaseDirectory().toFile(), fileSystem )
                .withCommandReaderFactory( dependencyResolver.resolveDependency( StorageEngineFactory.class ).commandReaderFactory() )
                .build();
        assertFalse( storeLogFiles.versionExists( 0 ) );
    }
}
