/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.routing;

import com.neo4j.causalclustering.common.Cluster;
import com.neo4j.test.causalclustering.ClusterConfig;
import com.neo4j.test.causalclustering.ClusterExtension;
import com.neo4j.test.causalclustering.ClusterFactory;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.junit.jupiter.api.parallel.Resources;

import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.exceptions.ClientException;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.SuppressOutputExtension;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;

import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

@TestDirectoryExtension
@ExtendWith( SuppressOutputExtension.class )
@ClusterExtension
@ResourceLock( Resources.SYSTEM_OUT )
public class WriteOnRRReproducerTest
{
    @Inject
    private static ClusterFactory clusterFactory;

    protected static Cluster cluster;

    protected static Driver readReplicaDriver;

    @BeforeAll
    static void beforeAll() throws Exception
    {
        ClusterConfig clusterConfig = ClusterConfig.clusterConfig()
                                                   .withNumberOfCoreMembers( 2 )
                                                   .withSharedCoreParam( GraphDatabaseSettings.routing_enabled, "true" )
                                                   .withNumberOfReadReplicas( 1 )
                                                   .withSharedReadReplicaParam( GraphDatabaseSettings.routing_enabled, "true" );

        cluster = clusterFactory.createCluster( clusterConfig );
        cluster.start();

        readReplicaDriver = getReadReplicaDriver( cluster );
    }

    @AfterAll
    static void afterAll()
    {
        readReplicaDriver.close();
        cluster.shutdown();
    }

    @Test
    void testWriteOnReadReplicaWithRoutingDisallowed()
    {
        try ( var session = readReplicaDriver.session() )
        {
            assertThatExceptionOfType( ClientException.class )
                    .isThrownBy( () -> session.run( "CREATE (n) RETURN id(n)" ).consume() )
                    .withMessage( "No write operations are allowed on this database. This is a read only Neo4j instance." );
        }
    }

    @Test
    void testWriteOnReadReplicaWithRoutingDisallowedWithInterpretedRuntime()
    {
        try ( var session = readReplicaDriver.session() )
        {
            assertThatExceptionOfType( ClientException.class )
                    .isThrownBy( () -> session.run( "CYPHER runtime=interpreted CREATE (n) RETURN id(n)" ).consume() )
                    .withMessage( "No write operations are allowed on this database. This is a read only Neo4j instance." );
        }
    }

    protected static Driver getReadReplicaDriver( Cluster cluster )
    {
        var readReplica = cluster.findAnyReadReplica();
        return driver( readReplica.directURI() );
    }

    static Driver driver( String uri )
    {
        return GraphDatabase.driver(
                uri,
                AuthTokens.none(),
                org.neo4j.driver.Config.builder()
                                       .withoutEncryption()
                                       .withMaxConnectionPoolSize( 3 )
                                       .build() );
    }
}
