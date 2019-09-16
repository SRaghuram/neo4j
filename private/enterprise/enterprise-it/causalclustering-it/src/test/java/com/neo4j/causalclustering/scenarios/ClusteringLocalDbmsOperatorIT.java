/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.scenarios;

import com.neo4j.causalclustering.common.Cluster;
import com.neo4j.causalclustering.common.ClusterMember;
import com.neo4j.causalclustering.common.ClusteredDatabaseContext;
import com.neo4j.causalclustering.core.CausalClusteringSettings;
import com.neo4j.dbms.LocalDbmsOperator;
import com.neo4j.test.causalclustering.ClusterConfig;
import com.neo4j.test.causalclustering.ClusterExtension;
import com.neo4j.test.causalclustering.ClusterFactory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import org.neo4j.dbms.database.DatabaseManager;
import org.neo4j.kernel.availability.DatabaseAvailabilityGuard;
import org.neo4j.kernel.database.TestDatabaseIdRepository;
import org.neo4j.test.extension.Inject;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ClusterExtension
@Disabled( "Creating and starting new databases does not work at the moment" )
class ClusteringLocalDbmsOperatorIT
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

        cluster.allMembers().forEach( member -> assertFalse( databaseContext( member ).isPresent() ) );

        cluster.allMembers().forEach( member -> localOperator( member ).startDatabase( dbName ) );
        cluster.allMembers().forEach( member -> assertTrue( availabilityGuard( member ).isAvailable() ) );

        cluster.allMembers().forEach( member -> localOperator( member ).stopDatabase( dbName ) );
        cluster.allMembers().forEach( member -> assertFalse( availabilityGuard( member ).isAvailable() ) );

        cluster.allMembers().forEach( member -> localOperator( member ).dropDatabase( dbName ) );
        cluster.allMembers().forEach( member -> assertFalse( databaseContext( member ).isPresent() ) );
    }

    private LocalDbmsOperator localOperator( ClusterMember member )
    {
        return member.defaultDatabase().getDependencyResolver().resolveDependency( LocalDbmsOperator.class );
    }

    private DatabaseManager<ClusteredDatabaseContext> databaseManager( ClusterMember member )
    {
        //noinspection unchecked
        return member.defaultDatabase()
                .getDependencyResolver()
                .resolveDependency( DatabaseManager.class );
    }

    private Optional<ClusteredDatabaseContext> databaseContext( ClusterMember member )
    {
        return databaseManager( member ).getDatabaseContext( TestDatabaseIdRepository.randomDatabaseId() );
    }

    private DatabaseAvailabilityGuard availabilityGuard( ClusterMember member )
    {
        Optional<ClusteredDatabaseContext> optContext = databaseContext( member );
        assertTrue( optContext.isPresent() );

        ClusteredDatabaseContext context = optContext.get();
        return context.database().getDatabaseAvailabilityGuard();
    }
}
