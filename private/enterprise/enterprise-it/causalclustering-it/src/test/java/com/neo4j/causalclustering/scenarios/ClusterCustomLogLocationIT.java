/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.scenarios;

import com.neo4j.causalclustering.common.Cluster;
import com.neo4j.causalclustering.readreplica.CatchupProcessFactory;
import com.neo4j.test.causalclustering.ClusterExtension;
import com.neo4j.test.causalclustering.ClusterFactory;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;

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

        var coreClusterMembers = cluster.coreMembers();
        for ( var coreClusterMember : coreClusterMembers )
        {
            var dependencyResolver = coreClusterMember.defaultDatabase().getDependencyResolver();
            var logFiles = dependencyResolver.resolveDependency( LogFiles.class );
            assertEquals( logFiles.logFilesDirectory().getFileName().toString(), coreClusterMember.defaultDatabase().databaseName() );
            assertTrue( logFiles.getLogFile().hasAnyEntries( 0 ) );

            logFileInStoreDirectoryDoesNotExist( coreClusterMember.databaseLayout(), dependencyResolver );
        }

        var readReplicas = cluster.readReplicas();
        for ( var readReplica : readReplicas )
        {
            var catchupProcessFactory = readReplica.resolveDependency( DEFAULT_DATABASE_NAME, CatchupProcessFactory.class );
            var uptoDateFuture = catchupProcessFactory.catchupProcessComponents()
                                                      .map( c -> c.catchupProcess().upToDateFuture() );
            if ( uptoDateFuture.isPresent() )
            {
                uptoDateFuture.get().get();
            }

            var dependencyResolver = readReplica.defaultDatabase().getDependencyResolver();
            var logFiles = dependencyResolver.resolveDependency( LogFiles.class );
            assertEquals( logFiles.logFilesDirectory().getFileName().toString(), readReplica.defaultDatabase().databaseName() );
            assertTrue( logFiles.getLogFile().hasAnyEntries( 0 ) );

            logFileInStoreDirectoryDoesNotExist( readReplica.databaseLayout(), dependencyResolver );
        }
    }

    private static void logFileInStoreDirectoryDoesNotExist( DatabaseLayout databaseLayout, DependencyResolver dependencyResolver ) throws IOException
    {
        FileSystemAbstraction fileSystem = dependencyResolver.resolveDependency( FileSystemAbstraction.class );
        LogFiles storeLogFiles = logFilesBasedOnlyBuilder( databaseLayout.databaseDirectory(), fileSystem )
                .withCommandReaderFactory( dependencyResolver.resolveDependency( StorageEngineFactory.class ).commandReaderFactory() )
                .build();
        assertFalse( storeLogFiles.getLogFile().versionExists( 0 ) );
    }
}
