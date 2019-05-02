/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.common;

import com.neo4j.causalclustering.core.CoreClusterMember;
import com.neo4j.causalclustering.discovery.RoleInfo;
import com.neo4j.causalclustering.read_replica.ReadReplica;
import org.hamcrest.Description;
import org.hamcrest.FeatureMatcher;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;

import java.net.URI;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.neo4j.exceptions.KernelException;
import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;

import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;
import static java.util.stream.Collectors.toSet;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.aMapWithSize;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.neo4j.test.assertion.Assert.assertEventually;

public class ClusterOverviewHelper
{
    public static void assertAllEventualOverviews( Cluster cluster, Matcher<List<MemberInfo>> expected ) throws InterruptedException, KernelException
    {
        assertAllEventualOverviews( cluster, expected, Collections.emptySet(), Collections.emptySet()  );
    }

    public static void assertAllEventualOverviews( Cluster cluster, Matcher<List<MemberInfo>> expected, Set<Integer> excludedCores,
            Set<Integer> excludedRRs ) throws InterruptedException, KernelException
    {
        for ( CoreClusterMember core : cluster.coreMembers() )
        {
            if ( !excludedCores.contains( core.serverId() ) )
            {
                assertEventualOverview( expected, core, "core" );
            }

        }

        for ( ReadReplica rr : cluster.readReplicas() )
        {
            if ( !excludedRRs.contains( rr.serverId() ) )
            {
                assertEventualOverview( expected, rr, "rr" );
            }
        }
    }

    public static void assertEventualOverview( Matcher<List<MemberInfo>> expected, CoreClusterMember core ) throws InterruptedException
    {
        assertEventualOverview( expected, core, "core" );
    }

    public static void assertEventualOverview( Matcher<List<MemberInfo>> expected, ReadReplica readReplica ) throws InterruptedException
    {
        assertEventualOverview( expected, readReplica, "rr" );
    }

    private static void assertEventualOverview( Matcher<List<MemberInfo>> expected, ClusterMember<? extends GraphDatabaseFacade> member, String role )
            throws InterruptedException
    {
        Function<List<MemberInfo>, String> printableMemberInfos =
                memberInfos -> memberInfos.stream().map( MemberInfo::toString ).collect( Collectors.joining( ", " ) );

        String message = String.format( "should have overview from %s %s, but view was ", role, member.serverId() );
        assertEventually( memberInfos -> message + printableMemberInfos.apply( memberInfos ),
                () -> clusterOverview( member.database() ), expected, 90, SECONDS );
    }

    public static Matcher<Iterable<? extends MemberInfo>> containsMemberAddresses( Collection<? extends ClusterMember<?>> members )
    {
        return containsInAnyOrder( members.stream().map( coreClusterMember ->
                new TypeSafeMatcher<MemberInfo>()
                {
                    @Override
                    protected boolean matchesSafely( MemberInfo item )
                    {
                        var expectedAddresses = Set.copyOf( coreClusterMember.clientConnectorAddresses().uriList() );
                        return expectedAddresses.equals( item.addresses );
                    }

                    @Override
                    public void describeTo( Description description )
                    {
                        description.appendText( "MemberInfo with addresses: " )
                                .appendValue( coreClusterMember.clientConnectorAddresses().boltAddress() );
                    }
                }
        ).collect( toList() ) );
    }

    public static Matcher<List<MemberInfo>> containsRole( RoleInfo expectedRole, DatabaseId databaseId, long expectedCount )
    {
        return new FeatureMatcher<>( equalTo( expectedCount ), expectedRole.toString(), "count" )
        {
            @Override
            protected Long featureValueOf( List<MemberInfo> overview )
            {
                return overview.stream().filter( info -> info.databases.get( databaseId ) == expectedRole ).count();
            }
        };
    }

    public static Matcher<List<MemberInfo>> doesNotContainRole( RoleInfo unexpectedRole, DatabaseId databaseId )
    {
        return containsRole( unexpectedRole, databaseId, 0 );
    }

    public static List<MemberInfo> clusterOverview( GraphDatabaseFacade db )
    {
        try ( var result = db.execute( "CALL dbms.cluster.overview()" ) )
        {
            return result.stream()
                    .map( ClusterOverviewHelper::createMemberInfo )
                    .collect( toList() );
        }
    }

    @SafeVarargs
    public static Matcher<Iterable<? extends MemberInfo>> containsAllMemberAddresses( Collection<? extends ClusterMember<?>>... members )
    {
        return containsMemberAddresses( Stream.of( members).flatMap( Collection::stream ).collect( toList() ) );
    }

    private static MemberInfo createMemberInfo( Map<String,Object> row )
    {
        assertThat( row, is( aMapWithSize( 4 ) ) );

        var addresses = extractAddresses( row );
        var databases = extractDatabases( row );

        return new MemberInfo( addresses, databases );
    }

    @SuppressWarnings( "unchecked" )
    private static Set<URI> extractAddresses( Map<String,Object> row )
    {
        var addressesObject = row.get( "addresses" );
        assertThat( addressesObject, instanceOf( List.class ) );
        return ((List<String>) addressesObject).stream()
                .map( URI::create )
                .collect( toSet() );
    }

    @SuppressWarnings( "unchecked" )
    private static Map<DatabaseId,RoleInfo> extractDatabases( Map<String,Object> row )
    {
        var databasesObject = row.get( "databases" );
        assertThat( databasesObject, instanceOf( Map.class ) );
        return ((Map<String,Object>) databasesObject).entrySet()
                .stream()
                .collect( toMap(
                        entry -> new DatabaseId( entry.getKey() ),
                        entry -> RoleInfo.valueOf( entry.getValue().toString() ) ) );
    }

    public static class MemberInfo
    {
        private final Set<URI> addresses;
        private final Map<DatabaseId,RoleInfo> databases;

        MemberInfo( Set<URI> addresses, Map<DatabaseId,RoleInfo> databases )
        {
            this.addresses = addresses;
            this.databases = databases;
        }

        @Override
        public String toString()
        {
            return "MemberInfo{" +
                   "addresses=" + addresses +
                   ", databases=" + databases +
                   '}';
        }
    }
}
