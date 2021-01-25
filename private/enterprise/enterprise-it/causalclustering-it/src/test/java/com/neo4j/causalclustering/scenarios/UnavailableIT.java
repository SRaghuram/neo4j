/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.scenarios;

import com.neo4j.causalclustering.common.Cluster;
import com.neo4j.causalclustering.common.ClusterMember;
import com.neo4j.test.causalclustering.ClusterExtension;
import com.neo4j.test.causalclustering.ClusterFactory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.availability.DatabaseAvailabilityGuard;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.extension.Inject;

import static com.neo4j.test.causalclustering.ClusterConfig.clusterConfig;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_METHOD;
import static org.neo4j.kernel.api.exceptions.Status.Database.DatabaseUnavailable;
import static org.neo4j.kernel.api.exceptions.Status.statusCodeOf;

@ClusterExtension
@TestInstance( PER_METHOD )
class UnavailableIT
{
    @Inject
    private ClusterFactory clusterFactory;

    private Cluster cluster;

    @BeforeEach
    void beforeEach() throws Exception
    {
        cluster = clusterFactory.createCluster( clusterConfig().withNumberOfCoreMembers( 3 ).withNumberOfReadReplicas( 0 ) );
        cluster.start();
    }

    @Test
    void shouldReturnUnavailableStatusWhenDoingLongOperation()
    {
        // given
        ClusterMember member = cluster.getCoreMemberByIndex( 1 );

        // when
        member.defaultDatabase().getDependencyResolver().resolveDependency( DatabaseAvailabilityGuard.class )
                .require( () -> "Not doing long operation" );

        // then
        var error = assertThrows( Exception.class, () ->
        {
            try ( Transaction tx = member.defaultDatabase().beginTx() )
            {
                tx.commit();
            }
        } );
        assertEquals( DatabaseUnavailable, statusCodeOf( error ) );
    }

    @Test
    void shouldReturnUnavailableStatusWhenShutdown()
    {
        // given
        ClusterMember member = cluster.getCoreMemberByIndex( 1 );

        // when
        GraphDatabaseAPI db = member.defaultDatabase();
        member.shutdown();

        // then
        var error = assertThrows( Exception.class, () ->
        {
            try ( Transaction tx = db.beginTx() )
            {
                tx.commit();
            }
        } );
        assertEquals( DatabaseUnavailable, statusCodeOf( error ) );
    }
}
