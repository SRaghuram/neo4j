/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.scenarios;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.neo4j.causalclustering.discovery.Cluster;
import org.neo4j.causalclustering.discovery.ClusterMember;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.availability.DatabaseAvailabilityGuard;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.causalclustering.ClusterRule;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.neo4j.kernel.api.exceptions.Status.statusCodeOf;

public class UnavailableIT
{
    @Rule
    public final ClusterRule clusterRule = new ClusterRule().withNumberOfCoreMembers( 3 );

    private Cluster<?> cluster;

    @Before
    public void setup() throws Exception
    {
        cluster = clusterRule.startCluster();
    }

    @Test
    public void shouldReturnUnavailableStatusWhenDoingLongOperation()
    {
        // given
        ClusterMember member = cluster.getCoreMemberById( 1 );

        // when
        member.database().getDependencyResolver().resolveDependency( DatabaseAvailabilityGuard.class )
                .require( () -> "Not doing long operation" );

        // then
        try ( Transaction tx = member.database().beginTx() )
        {
            tx.success();
            fail();
        }
        catch ( Exception e )
        {
            assertEquals( Status.General.DatabaseUnavailable, statusCodeOf( e ) );
        }
    }

    @Test
    public void shouldReturnUnavailableStatusWhenShutdown()
    {
        // given
        ClusterMember member = cluster.getCoreMemberById( 1 );

        // when
        GraphDatabaseAPI db = member.database();
        member.shutdown();

        // then
        try ( Transaction tx = db.beginTx() )
        {
            tx.success();
            fail();
        }
        catch ( Exception e )
        {
            assertEquals( Status.General.DatabaseUnavailable, statusCodeOf( e ) );
        }
    }
}
