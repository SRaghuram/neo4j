/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.scenarios;

import com.neo4j.causalclustering.common.Cluster;
import com.neo4j.causalclustering.core.CoreClusterMember;
import com.neo4j.causalclustering.helpers.ClassicNeo4jDatabase;
import com.neo4j.kernel.impl.store.format.highlimit.HighLimit;
import com.neo4j.restore.RestoreDatabaseCommand;
import com.neo4j.test.causalclustering.ClusterExtension;
import com.neo4j.test.causalclustering.ClusterFactory;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.nio.file.Path;
import java.util.concurrent.Callable;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.kernel.impl.store.format.aligned.PageAlignedV4_1;
import org.neo4j.kernel.impl.store.format.standard.Standard;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static com.neo4j.configuration.CausalClusteringSettings.raft_advertised_address;
import static com.neo4j.test.causalclustering.ClusterConfig.clusterConfig;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_METHOD;
import static org.neo4j.graphdb.Label.label;
import static org.neo4j.internal.helpers.collection.Iterables.count;
import static org.neo4j.test.assertion.Assert.assertEventually;

@TestDirectoryExtension
@ClusterExtension
@TestInstance( PER_METHOD )
class ConvertNonCausalClusteringStoreIT
{
    @Inject
    private ClusterFactory clusterFactory;
    @Inject
    private TestDirectory testDirectory;

    @ParameterizedTest( name = "Record format '{0}'" )
    @ValueSource( strings = {Standard.LATEST_NAME, HighLimit.NAME, PageAlignedV4_1.NAME} )
    void shouldReplicateTransactionToCoreMembers( String recordFormat ) throws Throwable
    {
        // given
        Path dbDir = testDirectory.cleanDirectory( "classic-db-" + recordFormat ).toPath();
        int classicNodeCount = 1024;
        Path classicNeo4jDatabase = createNeoDatabase( dbDir, recordFormat, classicNodeCount ).layout().databaseDirectory();

        Cluster cluster = createCluster( recordFormat );

        for ( CoreClusterMember core : cluster.coreMembers() )
        {
            var databaseName = GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
            new RestoreDatabaseCommand( testDirectory.getFileSystem(), classicNeo4jDatabase, core.config(), databaseName, true, false ).execute();
        }

        cluster.start();

        // when
        cluster.coreTx( ( coreDB, tx ) ->
        {
            Node node = tx.createNode( label( "boo" ) );
            node.setProperty( "foobar", "baz_bat" );
            tx.commit();
        } );

        cluster.addReadReplicaWithIdAndRecordFormat( 4, recordFormat ).start();

        // then
        for ( final CoreClusterMember server : cluster.coreMembers() )
        {
            GraphDatabaseFacade db = server.defaultDatabase();

            try ( Transaction tx = db.beginTx() )
            {
                Callable<Long> nodeCount = () -> count( tx.getAllNodes() );

                Config config = db.getDependencyResolver().resolveDependency( Config.class );

                assertEventually( "node to appear on core server " + config.get( raft_advertised_address ), nodeCount,
                        v -> v > classicNodeCount, 15, SECONDS );

                assertEquals( classicNodeCount + 1, count( tx.getAllNodes() ) );

                tx.commit();
            }
        }
    }

    private Cluster createCluster( String recordFormat )
    {
        var clusterConfig = clusterConfig()
                .withNumberOfCoreMembers( 3 )
                .withNumberOfReadReplicas( 0 )
                .withRecordFormat( recordFormat );

        return clusterFactory.createCluster( clusterConfig );
    }

    private ClassicNeo4jDatabase createNeoDatabase( Path dbDir, String recordFormat, int classicNodeCount ) throws IOException
    {
        return ClassicNeo4jDatabase.builder( dbDir, testDirectory.getFileSystem() )
                .transactionLogsInDatabaseFolder()
                .amountOfNodes( classicNodeCount ).recordFormats( recordFormat ).build();
    }
}
