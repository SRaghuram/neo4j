/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.scenarios;

import com.neo4j.causalclustering.common.Cluster;
import com.neo4j.causalclustering.common.ClusterMember;
import com.neo4j.causalclustering.common.ClusteredDatabaseContext;
import com.neo4j.causalclustering.common.ClusteredDatabaseManager;
import com.neo4j.causalclustering.core.CausalClusteringSettings;
import com.neo4j.dbms.LocalOperator;
import com.neo4j.test.causalclustering.ClusterConfig;
import com.neo4j.test.causalclustering.ClusterExtension;
import com.neo4j.test.causalclustering.ClusterFactory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import org.neo4j.dbms.database.DatabaseManager;
import org.neo4j.kernel.availability.DatabaseAvailabilityGuard;
import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.extension.Inject;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ClusterExtension
@Disabled( "Creating and starting new databases does not work at the moment" )
class ClusteringLocalOperatorIT
{
    @Inject
    private ClusterFactory clusterFactory;

    private Cluster cluster;

    private final ClusterConfig clusterConfig = ClusterConfig
            .clusterConfig()
            .withNumberOfCoreMembers( 3 )
            .withSharedCoreParam( CausalClusteringSettings.minimum_core_cluster_size_at_formation, "3" )
            .withNumberOfReadReplicas( 3 )
            .withTimeout( 1000, SECONDS );

    @BeforeEach
    void setup() throws Exception
    {
        cluster = clusterFactory.createCluster( clusterConfig );
        cluster.start();
    }

    @Test
    void shouldSupportDatabaseOperations()
    {
        String dbName = "my.db";

        cluster.allMembers().forEach( member -> assertFalse( databaseContext( member, dbName ).isPresent() ) );

        cluster.allMembers().forEach( member -> localOperator( member ).createDatabase( dbName ) );
        cluster.allMembers().forEach( member -> assertTrue( databaseContext( member, dbName ).isPresent() ) );

        cluster.allMembers().forEach( member -> localOperator( member ).startDatabase( dbName ) );
        cluster.allMembers().forEach( member -> assertTrue( availabilityGuard( member, dbName ).isAvailable() ) );

        cluster.allMembers().forEach( member -> localOperator( member ).stopDatabase( dbName ) );
        cluster.allMembers().forEach( member -> assertFalse( availabilityGuard( member, dbName ).isAvailable() ) );

        cluster.allMembers().forEach( member -> localOperator( member ).dropDatabase( dbName ) );
        cluster.allMembers().forEach( member -> assertFalse( databaseContext( member, dbName ).isPresent() ) );
    }

    private LocalOperator localOperator( ClusterMember<? extends GraphDatabaseAPI> member )
    {
        return member.database().getDependencyResolver().resolveDependency( LocalOperator.class );
    }

    private ClusteredDatabaseManager<? extends ClusteredDatabaseContext> databaseManager( ClusterMember<? extends GraphDatabaseAPI> member )
    {
        return (ClusteredDatabaseManager<? extends ClusteredDatabaseContext>) member
                .database()
                .getDependencyResolver()
                .resolveDependency( DatabaseManager.class );
    }

    private Optional<? extends ClusteredDatabaseContext> databaseContext( ClusterMember<? extends GraphDatabaseAPI> member, String databaseName )
    {
        return databaseManager( member ).getDatabaseContext( new DatabaseId( databaseName ) );
    }

    private DatabaseAvailabilityGuard availabilityGuard( ClusterMember<? extends GraphDatabaseAPI> member, String databaseName )
    {
        Optional<? extends ClusteredDatabaseContext> optContext = databaseContext( member, databaseName );
        assertTrue( optContext.isPresent() );

        ClusteredDatabaseContext context = optContext.get();
        return context.database().getDatabaseAvailabilityGuard();
    }
}
