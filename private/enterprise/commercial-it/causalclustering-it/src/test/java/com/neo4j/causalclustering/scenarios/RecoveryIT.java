/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.scenarios;

import com.neo4j.causalclustering.common.Cluster;
import com.neo4j.causalclustering.core.CoreClusterMember;
import com.neo4j.test.causalclustering.ClusterRule;
import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.neo4j.configuration.Config;
import org.neo4j.consistency.ConsistencyCheckService;
import org.neo4j.graphdb.Node;
import org.neo4j.helpers.progress.ProgressMonitorFactory;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.test.DbRepresentation;

import static java.util.stream.Collectors.toSet;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertTrue;
import static org.neo4j.graphdb.Label.label;
import static org.neo4j.test.assertion.Assert.assertEventually;

public class RecoveryIT
{
    @Rule
    public final ClusterRule clusterRule = new ClusterRule()
            .withNumberOfCoreMembers( 3 )
            .withNumberOfReadReplicas( 0 );

    @Test
    public void shouldBeConsistentAfterShutdown() throws Exception
    {
        // given
        Cluster<?> cluster = clusterRule.startCluster();

        fireSomeLoadAtTheCluster( cluster );

        Set<File> storeDirs = cluster.coreMembers().stream().map( CoreClusterMember::databaseDirectory ).collect( toSet() );

        assertEventually( "All cores have the same data",
                () -> cluster.coreMembers().stream().map( RecoveryIT::dbRepresentation ).collect( toSet() ).size(),
                equalTo( 1 ), 10, TimeUnit.SECONDS );

        // when
        cluster.shutdown();

        // then
        storeDirs.forEach( RecoveryIT::assertConsistent );
    }

    @Test
    public void singleServerWithinClusterShouldBeConsistentAfterRestart() throws Exception
    {
        // given
        Cluster<?> cluster = clusterRule.startCluster();
        int clusterSize = cluster.numberOfCoreMembersReportedByTopology();

        fireSomeLoadAtTheCluster( cluster );

        Set<File> storeDirs = cluster.coreMembers().stream().map( CoreClusterMember::databaseDirectory ).collect( toSet() );

        // when
        for ( int i = 0; i < clusterSize; i++ )
        {
            cluster.removeCoreMemberWithServerId( i );
            fireSomeLoadAtTheCluster( cluster );
            cluster.addCoreMemberWithId( i ).start();
        }

        // then
        assertEventually( "All cores have the same data",
                () -> cluster.coreMembers().stream().map( RecoveryIT::dbRepresentation ).collect( toSet() ).size(),
                equalTo( 1 ), 10, TimeUnit.SECONDS );

        cluster.shutdown();

        storeDirs.forEach( RecoveryIT::assertConsistent );
    }

    private static DbRepresentation dbRepresentation( CoreClusterMember member )
    {
        return  DbRepresentation.of( member.database() );
    }

    private static void assertConsistent( File storeDir )
    {
        ConsistencyCheckService.Result result;
        try
        {
            result = new ConsistencyCheckService().runFullConsistencyCheck( DatabaseLayout.of( storeDir ), Config.defaults(),
                    ProgressMonitorFactory.NONE, NullLogProvider.getInstance(), true );
        }
        catch ( Exception e )
        {
            throw new RuntimeException( e );
        }

        assertTrue( result.isSuccessful() );
    }

    private static void fireSomeLoadAtTheCluster( Cluster<?> cluster ) throws Exception
    {
        for ( int i = 0; i < cluster.numberOfCoreMembersReportedByTopology(); i++ )
        {
            final String prop = "val" + i;
            cluster.coreTx( ( db, tx ) ->
            {
                Node node = db.createNode( label( "demo" ) );
                node.setProperty( "server", prop );
                tx.success();
            } );
        }
    }
}
