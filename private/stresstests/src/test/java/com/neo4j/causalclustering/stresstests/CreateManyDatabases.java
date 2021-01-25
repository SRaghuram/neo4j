/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.stresstests;

import com.neo4j.causalclustering.common.Cluster;
import com.neo4j.causalclustering.common.ClusterMember;
import com.neo4j.causalclustering.common.ClusterOverviewHelper;
import com.neo4j.helper.Workload;
import org.assertj.core.api.Condition;
import org.assertj.core.api.HamcrestCondition;
import org.hamcrest.Matchers;

import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import org.neo4j.configuration.helpers.NormalizedDatabaseName;
import org.neo4j.graphdb.Transaction;
import org.neo4j.logging.Log;

import static com.neo4j.causalclustering.common.CausalClusteringTestHelpers.assertDatabaseEventuallyStarted;
import static com.neo4j.causalclustering.common.CausalClusteringTestHelpers.createDatabase;
import static com.neo4j.causalclustering.common.ClusterOverviewHelper.assertAllEventualOverviews;
import static com.neo4j.causalclustering.common.ClusterOverviewHelper.containsAllMemberAddresses;
import static com.neo4j.causalclustering.common.ClusterOverviewHelper.containsRole;
import static com.neo4j.causalclustering.discovery.RoleInfo.FOLLOWER;
import static com.neo4j.causalclustering.discovery.RoleInfo.LEADER;
import static com.neo4j.causalclustering.discovery.RoleInfo.READ_REPLICA;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.apache.commons.lang3.RandomStringUtils.random;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;
import static org.neo4j.configuration.helpers.DatabaseNameValidator.MAXIMUM_DATABASE_NAME_LENGTH;
import static org.neo4j.configuration.helpers.DatabaseNameValidator.validateExternalDatabaseName;
import static org.neo4j.graphdb.Label.label;
import static org.neo4j.test.assertion.Assert.assertEventually;

public class CreateManyDatabases extends Workload
{
    private final Cluster cluster;
    private final Set<String> allDatabases = new HashSet<>();
    private final int numberOfDatabases;
    private final Log log;

    CreateManyDatabases( Control control, Resources resources, Config config )
    {
        super( control );

        this.cluster = resources.cluster();
        this.numberOfDatabases = config.numberOfDatabases();
        this.log = resources.logProvider().getLog( getClass() );
    }

    @Override
    public void prepare() throws Exception
    {
        assertDatabaseEventuallyStarted( SYSTEM_DATABASE_NAME, cluster );
        allDatabases.add( SYSTEM_DATABASE_NAME );

        assertDatabaseEventuallyStarted( DEFAULT_DATABASE_NAME, cluster );
        allDatabases.add( DEFAULT_DATABASE_NAME );
    }

    @Override
    protected void doWork() throws Exception
    {
        if ( allDatabases.size() >= numberOfDatabases )
        {
            control.onFinish();
            return;
        }

        long start = System.currentTimeMillis();
        String databaseName = createDatabaseWithRandomName();
        assertDatabaseEventuallyStarted( databaseName, cluster );
        long elapsed = System.currentTimeMillis() - start;

        allDatabases.add( databaseName );

        log.info( "Started database " + allDatabases.size() + " with name '" + databaseName + "' in " + elapsed + " ms." );

        // it is on purpose that we check all databases every time, for extra validation,
        // even though it would seem more efficient to just check the one we just added
        checkOverviews( allDatabases );
    }

    @Override
    public void validate() throws Exception
    {
        checkOverviews( allDatabases );

        // check so that we actually can write ...
        for ( String databaseName : allDatabases )
        {
            cluster.coreTx( databaseName, ( db, tx ) ->
            {
                tx.createNode( label( databaseName ) ).setProperty( databaseName, databaseName );
                tx.commit();
            } );
        }

        // ... and read from all the databases
        for ( String databaseName : allDatabases )
        {
            for ( ClusterMember member : cluster.allMembers() )
            {
                assertEventually( () ->
                {
                    try ( Transaction tx = member.database( databaseName ).beginTx() )
                    {
                        return tx.findNode( label( databaseName ), databaseName, databaseName );
                    }
                }, new Condition<>( Objects::nonNull, "Should be not null." ), 1, MINUTES );
            }
        }
    }

    private String createDatabaseWithRandomName() throws Exception
    {
        NormalizedDatabaseName databaseName = new NormalizedDatabaseName(
                random( 1, true, false ) +
                random( MAXIMUM_DATABASE_NAME_LENGTH - 1, true, true ) );

        validateExternalDatabaseName( databaseName );
        createDatabase( databaseName.name(), cluster );

        return databaseName.name();
    }

    private void checkOverviews( Set<String> createdDatabases )
    {
        for ( String createdDatabase : createdDatabases )
        {
            Condition<List<ClusterOverviewHelper.MemberInfo>> expected = new HamcrestCondition<>( Matchers.allOf(
                    containsAllMemberAddresses( cluster.coreMembers(), cluster.readReplicas() ),
                    containsRole( LEADER, createdDatabase, 1 ),
                    containsRole( FOLLOWER, createdDatabase, cluster.coreMembers().size() - 1 ),
                    containsRole( READ_REPLICA, createdDatabase, cluster.readReplicas().size() )
            ) );

            assertAllEventualOverviews( cluster, expected );
        }
    }
}
