/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.scenarios;

import com.neo4j.causalclustering.common.Cluster;
import com.neo4j.causalclustering.core.CoreClusterMember;
import com.neo4j.causalclustering.core.CoreGraphDatabase;
import com.neo4j.causalclustering.helpers.ClassicNeo4jDatabase;
import com.neo4j.kernel.impl.store.format.highlimit.HighLimit;
import com.neo4j.test.causalclustering.ClusterRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.function.ThrowingSupplier;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.kernel.impl.store.format.standard.Standard;
import org.neo4j.restore.RestoreDatabaseCommand;
import org.neo4j.test.rule.TestDirectory;

import static com.neo4j.causalclustering.core.CausalClusteringSettings.raft_advertised_address;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.hamcrest.Matchers.greaterThan;
import static org.junit.Assert.assertEquals;
import static org.neo4j.graphdb.Label.label;
import static org.neo4j.helpers.collection.Iterables.count;
import static org.neo4j.test.assertion.Assert.assertEventually;

@RunWith( Parameterized.class )
public class ConvertNonCausalClusteringStoreIT
{
    @Rule
    public final ClusterRule clusterRule = new ClusterRule()
            .withNumberOfCoreMembers( 3 )
            .withNumberOfReadReplicas( 0 );

    @Parameterized.Parameter()
    public String recordFormat;

    @Parameterized.Parameters( name = "Record format {0}" )
    public static Collection<Object> data()
    {
        return Arrays.asList( new Object[]{Standard.LATEST_NAME, HighLimit.NAME} );
    }

    @Test
    public void shouldReplicateTransactionToCoreMembers() throws Throwable
    {
        // given
        TestDirectory testDirectory = clusterRule.testDirectory();
        File dbDir = testDirectory.cleanDirectory( "classic-db-" + recordFormat );
        int classicNodeCount = 1024;
        File classicNeo4jDatabase = createNeoDatabase( dbDir, classicNodeCount ).layout().databaseDirectory();

        Cluster cluster = this.clusterRule.withRecordFormat( recordFormat ).createCluster();

        try ( DefaultFileSystemAbstraction fileSystem = new DefaultFileSystemAbstraction() )
        {
            for ( CoreClusterMember core : cluster.coreMembers() )
            {
                var databaseId = new DatabaseId( core.settingValue( GraphDatabaseSettings.default_database.name() ) );
                new RestoreDatabaseCommand( fileSystem, classicNeo4jDatabase, core.config(), databaseId, true ).execute();
            }
        }

        cluster.start();

        // when
        cluster.coreTx( ( coreDB, tx ) ->
        {
            Node node = coreDB.createNode( label( "boo" ) );
            node.setProperty( "foobar", "baz_bat" );
            tx.success();
        } );

        cluster.addReadReplicaWithIdAndRecordFormat( 4, recordFormat ).start();

        // then
        for ( final CoreClusterMember server : cluster.coreMembers() )
        {
            CoreGraphDatabase db = server.database();

            try ( Transaction tx = db.beginTx() )
            {
                ThrowingSupplier<Long,Exception> nodeCount = () -> count( db.getAllNodes() );

                Config config = db.getDependencyResolver().resolveDependency( Config.class );

                assertEventually( "node to appear on core server " + config.get( raft_advertised_address ), nodeCount,
                        greaterThan( (long) classicNodeCount ), 15, SECONDS );

                assertEquals( classicNodeCount + 1, count( db.getAllNodes() ) );

                tx.success();
            }
        }
    }

    private ClassicNeo4jDatabase createNeoDatabase( File dbDir, int classicNodeCount ) throws IOException
    {
        try ( DefaultFileSystemAbstraction fileSystem = new DefaultFileSystemAbstraction() )
        {
            return ClassicNeo4jDatabase.builder( dbDir, fileSystem )
                    .transactionLogsInDatabaseFolder()
                    .amountOfNodes( classicNodeCount ).recordFormats( recordFormat ).build();
        }
    }
}
