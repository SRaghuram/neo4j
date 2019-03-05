/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.scenarios;

import com.neo4j.causalclustering.common.Cluster;
import com.neo4j.causalclustering.core.CoreClusterMember;
import com.neo4j.causalclustering.read_replica.ReadReplica;
import com.neo4j.test.causalclustering.ClusterRule;
import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.Collection;

import org.neo4j.common.DependencyResolver;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.impl.transaction.log.files.LogFiles;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.neo4j.kernel.impl.transaction.log.files.LogFilesBuilder.logFilesBasedOnlyBuilder;

public class ClusterCustomLogLocationIT
{
    @Rule
    public final ClusterRule clusterRule = new ClusterRule()
            .withNumberOfCoreMembers( 3 )
            .withNumberOfReadReplicas( 2 );

    @Test
    public void clusterWithCustomTransactionLogLocation() throws Exception
    {
        Cluster cluster = clusterRule.startCluster();

        for ( int i = 0; i < 10; i++ )
        {
            cluster.coreTx( ( db, tx ) ->
            {
                db.createNode();
                tx.success();
            } );
        }

        Collection<CoreClusterMember> coreClusterMembers = cluster.coreMembers();
        for ( CoreClusterMember coreClusterMember : coreClusterMembers )
        {
            DependencyResolver dependencyResolver = coreClusterMember.database().getDependencyResolver();
            LogFiles logFiles = dependencyResolver.resolveDependency( LogFiles.class );
            assertEquals( logFiles.logFilesDirectory().getName(), coreClusterMember.database().databaseLayout().getDatabaseName() );
            assertTrue( logFiles.hasAnyEntries( 0 ) );

            logFileInStoreDirectoryDoesNotExist( coreClusterMember.databaseDirectory(), dependencyResolver );
        }

        Collection<ReadReplica> readReplicas = cluster.readReplicas();
        for ( ReadReplica readReplica : readReplicas )
        {
            readReplica.txPollingClient().upToDateFuture().get();
            DependencyResolver dependencyResolver = readReplica.database().getDependencyResolver();
            LogFiles logFiles = dependencyResolver.resolveDependency( LogFiles.class );
            assertEquals( logFiles.logFilesDirectory().getName(), readReplica.database().databaseLayout().getDatabaseName() );
            assertTrue( logFiles.hasAnyEntries( 0 ) );

            logFileInStoreDirectoryDoesNotExist( readReplica.databaseDirectory(), dependencyResolver );
        }
    }

    private static void logFileInStoreDirectoryDoesNotExist( File storeDir, DependencyResolver dependencyResolver ) throws IOException
    {
        FileSystemAbstraction fileSystem = dependencyResolver.resolveDependency( FileSystemAbstraction.class );
        LogFiles storeLogFiles = logFilesBasedOnlyBuilder( storeDir, fileSystem ).build();
        assertFalse( storeLogFiles.versionExists( 0 ) );
    }
}
