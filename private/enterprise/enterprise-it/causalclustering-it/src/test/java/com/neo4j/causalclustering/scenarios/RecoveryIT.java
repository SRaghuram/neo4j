/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.scenarios;

import com.neo4j.causalclustering.common.Cluster;
import com.neo4j.causalclustering.core.CoreClusterMember;
import com.neo4j.test.causalclustering.ClusterExtension;
import com.neo4j.test.causalclustering.ClusterFactory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.util.concurrent.TimeUnit;

import org.neo4j.configuration.Config;
import org.neo4j.consistency.ConsistencyCheckService;
import org.neo4j.consistency.checking.full.ConsistencyCheckIncompleteException;
import org.neo4j.internal.helpers.progress.ProgressMonitorFactory;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.test.DbRepresentation;
import org.neo4j.test.extension.Inject;

import static com.neo4j.test.causalclustering.ClusterConfig.clusterConfig;
import static java.util.stream.Collectors.toSet;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_METHOD;
import static org.neo4j.graphdb.Label.label;
import static org.neo4j.test.assertion.Assert.assertEventually;
import static org.neo4j.test.conditions.Conditions.equalityCondition;

@ClusterExtension
@TestInstance( PER_METHOD )
class RecoveryIT
{
    private static final int CORE_COUNT = 3;
    private static final int READ_REPLICA_COUNT = 0;

    @Inject
    private ClusterFactory clusterFactory;

    private Cluster cluster;

    @BeforeEach
    void beforeEach() throws Exception
    {
        cluster = clusterFactory.createCluster( clusterConfig().withNumberOfCoreMembers( CORE_COUNT ).withNumberOfReadReplicas( READ_REPLICA_COUNT ) );
        cluster.start();
    }

    @Test
    void shouldBeConsistentAfterShutdown() throws Exception
    {
        // given
        fireSomeLoadAtTheCluster( cluster );

        var coreDatabaseLayouts = cluster.coreMembers().stream().map( CoreClusterMember::databaseLayout ).collect( toSet() );

        assertEventually( "All cores have the same data",
                () -> cluster.coreMembers().stream().map( RecoveryIT::dbRepresentation ).collect( toSet() ).size(),
                equalityCondition( 1 ), 10, TimeUnit.SECONDS );

        // when
        cluster.shutdown();

        // then
        coreDatabaseLayouts.forEach( RecoveryIT::assertConsistent );
    }

    @Test
    void singleServerWithinClusterShouldBeConsistentAfterRestart() throws Exception
    {
        // given
        fireSomeLoadAtTheCluster( cluster );

        var coreDatabaseLayouts = cluster.coreMembers().stream().map( CoreClusterMember::databaseLayout ).collect( toSet() );

        // when
        for ( var i = 0; i < CORE_COUNT; i++ )
        {
            cluster.removeCoreMemberWithIndex( i );
            fireSomeLoadAtTheCluster( cluster );
            cluster.addCoreMemberWithIndex( i ).start();
        }

        // then
        assertEventually( "All cores have the same data",
                () -> cluster.coreMembers().stream().map( RecoveryIT::dbRepresentation ).collect( toSet() ).size(),
                equalityCondition( 1 ), 10, TimeUnit.SECONDS );

        cluster.shutdown();

        coreDatabaseLayouts.forEach( RecoveryIT::assertConsistent );
    }

    private static DbRepresentation dbRepresentation( CoreClusterMember member )
    {
        return  DbRepresentation.of( member.defaultDatabase() );
    }

    private static void assertConsistent( DatabaseLayout databaseLayout )
    {
        try
        {
            var consistencyCheckResult = new ConsistencyCheckService().runFullConsistencyCheck( databaseLayout, Config.defaults(),
                    ProgressMonitorFactory.NONE, NullLogProvider.getInstance(), true );

            assertTrue( consistencyCheckResult.isSuccessful() );
        }
        catch ( ConsistencyCheckIncompleteException e )
        {
            throw new RuntimeException( e );
        }
    }

    private static void fireSomeLoadAtTheCluster( Cluster cluster ) throws Exception
    {
        for ( var i = 0; i < CORE_COUNT; i++ )
        {
            var prop = "val" + i;
            cluster.coreTx( ( db, tx ) ->
            {
                var node = tx.createNode( label( "demo" ) );
                node.setProperty( "server", prop );
                tx.commit();
            } );
        }
    }
}
