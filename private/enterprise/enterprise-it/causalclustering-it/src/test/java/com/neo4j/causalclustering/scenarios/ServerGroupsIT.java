/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.scenarios;

import com.neo4j.test.causalclustering.ClusterExtension;
import com.neo4j.test.causalclustering.ClusterFactory;
import org.assertj.core.api.HamcrestCondition;
import org.hamcrest.Description;
import org.hamcrest.TypeSafeMatcher;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.IntFunction;

import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.test.extension.Inject;

import static com.neo4j.configuration.CausalClusteringSettings.server_groups;
import static com.neo4j.test.causalclustering.ClusterConfig.clusterConfig;
import static java.lang.String.format;
import static java.util.Arrays.asList;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_METHOD;
import static org.neo4j.test.assertion.Assert.assertEventually;

@ClusterExtension
@TestInstance( PER_METHOD )
class ServerGroupsIT
{
    @Inject
    private ClusterFactory clusterFactory;

    @Test
    void shouldUpdateGroupsOnStart() throws Exception
    {
        var suffix = new AtomicReference<>( "before" );

        var instanceCoreParams = Map.<String,IntFunction<String>>of(
                server_groups.name(), id -> String.join( ", ", makeCoreGroups( suffix.get(), id ) ) );

        var instanceReplicaParams = Map.<String,IntFunction<String>>of(
                server_groups.name(), id -> String.join( ", ", makeReplicaGroups( suffix.get(), id ) ) );

        var clusterConfig = clusterConfig()
                .withNumberOfCoreMembers( 3 )
                .withNumberOfReadReplicas( 3 )
                .withInstanceCoreParams( instanceCoreParams )
                .withInstanceReadReplicaParams( instanceReplicaParams );

        var cluster = clusterFactory.createCluster( clusterConfig );

        // when
        cluster.start();

        // then
        var expected = new ArrayList<List<String>>();
        for ( var core : cluster.coreMembers() )
        {
            expected.add( makeCoreGroups( suffix.get(), core.index() ) );
            expected.add( makeReplicaGroups( suffix.get(), core.index() ) );
        }

        for ( var core : cluster.coreMembers() )
        {
            assertEventually( core + " should have groups", () -> getServerGroups( core.defaultDatabase() ),
                    new HamcrestCondition<>( new GroupsMatcher( expected ) ), 30, SECONDS );
        }

        // when
        expected.remove( makeCoreGroups( suffix.get(), 1 ) );
        expected.remove( makeReplicaGroups( suffix.get(), 2 ) );
        cluster.getCoreMemberByIndex( 1 ).shutdown();
        cluster.getReadReplicaByIndex( 2 ).shutdown();

        suffix.set( "after" ); // should update groups of restarted servers
        cluster.addCoreMemberWithIndex( 1 ).start();
        cluster.addReadReplicaWithIndex( 2 ).start();
        expected.add( makeCoreGroups( suffix.get(), 1 ) );
        expected.add( makeReplicaGroups( suffix.get(), 2 ) );

        // then
        for ( var core : cluster.coreMembers() )
        {
            assertEventually( core + " should have groups", () -> getServerGroups( core.defaultDatabase() ),
                    new HamcrestCondition<>( new GroupsMatcher( expected ) ), 30, SECONDS );
        }
    }

    private static class GroupsMatcher extends TypeSafeMatcher<List<List<String>>>
    {
        final List<List<String>> expected;

        GroupsMatcher( List<List<String>> expected )
        {
            this.expected = expected;
        }

        @Override
        protected boolean matchesSafely( List<List<String>> actual )
        {
            if ( actual.size() != expected.size() )
            {
                return false;
            }

            for ( var actualGroups : actual )
            {
                var matched = false;
                for ( var expectedGroups : expected )
                {
                    if ( actualGroups.size() != expectedGroups.size() )
                    {
                        continue;
                    }

                    if ( !actualGroups.containsAll( expectedGroups ) )
                    {
                        continue;
                    }

                    matched = true;
                    break;
                }

                if ( !matched )
                {
                    return false;
                }
            }

            return true;
        }

        @Override
        public void describeTo( Description description )
        {
            description.appendText( expected.toString() );
        }
    }

    private static List<String> makeCoreGroups( String suffix, int id )
    {
        return asList( format( "core-%d-%s", id, suffix ), "core" );
    }

    private static List<String> makeReplicaGroups( String suffix, int id )
    {
        return asList( format( "replica-%d-%s", id, suffix ), "replica" );
    }

    private static List<List<String>> getServerGroups( GraphDatabaseFacade db )
    {
        var serverGroups = new ArrayList<List<String>>();
        try ( var transaction = db.beginTx();
              var result = transaction.execute( "CALL dbms.cluster.overview" ) )
        {
            while ( result.hasNext() )
            {
                @SuppressWarnings( "unchecked" )
                var groups = (List<String>) result.next().get( "groups" );
                serverGroups.add( groups );
            }
        }
        return serverGroups;
    }
}
