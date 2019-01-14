/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.scenarios;

import org.hamcrest.Description;
import org.hamcrest.FeatureMatcher;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;

import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.neo4j.causalclustering.discovery.Cluster;
import org.neo4j.causalclustering.discovery.ClusterMember;
import org.neo4j.causalclustering.discovery.CoreClusterMember;
import org.neo4j.causalclustering.discovery.ReadReplica;
import org.neo4j.causalclustering.discovery.RoleInfo;
import org.neo4j.causalclustering.discovery.procedures.ClusterOverviewProcedure;
import org.neo4j.collection.RawIterator;
import org.neo4j.internal.kernel.api.Kernel;
import org.neo4j.internal.kernel.api.Transaction;
import org.neo4j.internal.kernel.api.exceptions.KernelException;
import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.internal.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.kernel.api.security.AnonymousContext;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;

import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.Collectors.toList;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.neo4j.helpers.collection.Iterators.asSet;
import static org.neo4j.internal.kernel.api.procs.ProcedureSignature.procedureName;
import static org.neo4j.test.assertion.Assert.assertEventually;

public class ClusterOverviewHelper
{
    public static void assertAllEventualOverviews( Cluster<?> cluster, Matcher<List<MemberInfo>> expected ) throws KernelException, InterruptedException
    {
        assertAllEventualOverviews( cluster, expected, Collections.emptySet(), Collections.emptySet()  );
    }

    public static void assertAllEventualOverviews( Cluster<?> cluster, Matcher<List<MemberInfo>> expected, Set<Integer> excludedCores,
            Set<Integer> excludedRRs ) throws KernelException, InterruptedException
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

    static void assertEventualOverview( Matcher<List<MemberInfo>> expected, ClusterMember<? extends GraphDatabaseFacade> member, String role )
            throws KernelException, InterruptedException
    {
        Function<List<MemberInfo>, String> printableMemberInfos =
                memberInfos -> memberInfos.stream().map( MemberInfo::toString ).collect( Collectors.joining( ", " ) );

        String message = String.format( "should have overview from %s %s, but view was ", role, member.serverId() );
        assertEventually( memberInfos -> message + printableMemberInfos.apply( memberInfos ),
                () -> clusterOverview( member.database() ), expected, 90, SECONDS );
    }

    public static Matcher<Iterable<? extends MemberInfo>> containsMemberAddresses( Collection<? extends ClusterMember> members )
    {
        return containsInAnyOrder( members.stream().map( coreClusterMember ->
                new TypeSafeMatcher<MemberInfo>()
                {
                    @Override
                    protected boolean matchesSafely( MemberInfo item )
                    {
                        Set<String> addresses = asSet( item.addresses );
                        for ( URI uri : coreClusterMember.clientConnectorAddresses().uriList() )
                        {
                            if ( !addresses.contains( uri.toString() ) )
                            {
                                return false;
                            }
                        }
                        return true;
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

    public static Matcher<List<MemberInfo>> containsRole( RoleInfo expectedRole, long expectedCount )
    {
        return new FeatureMatcher<List<MemberInfo>,Long>( equalTo( expectedCount ), expectedRole.name(), "count" )
        {
            @Override
            protected Long featureValueOf( List<MemberInfo> overview )
            {
                return overview.stream().filter( info -> info.role == expectedRole ).count();
            }
        };
    }

    public static Matcher<List<MemberInfo>> doesNotContainRole( RoleInfo unexpectedRole )
    {
       return containsRole( unexpectedRole, 0 );
    }

    @SuppressWarnings( "unchecked" )
    public static List<MemberInfo> clusterOverview( GraphDatabaseFacade db )
            throws TransactionFailureException, ProcedureException
    {
        Kernel kernel = db.getDependencyResolver().resolveDependency( Kernel.class );

        List<MemberInfo> infos = new ArrayList<>();
        try ( Transaction tx = kernel.beginTransaction( Transaction.Type.implicit, AnonymousContext.read() ) )
        {
            RawIterator<Object[],ProcedureException> itr =
                    tx.procedures().procedureCallRead( procedureName( "dbms", "cluster", ClusterOverviewProcedure.PROCEDURE_NAME ), null );

            while ( itr.hasNext() )
            {
                Object[] row = itr.next();
                List<String> addresses = (List<String>) row[1];
                infos.add( new MemberInfo( addresses.toArray( new String[addresses.size()] ), RoleInfo.valueOf( (String) row[2] ) ) );
            }
            return infos;
        }
    }

    @SafeVarargs
    public static Matcher<Iterable<? extends MemberInfo>> containsAllMemberAddresses(
            Collection<? extends ClusterMember>... members )
    {
        return containsMemberAddresses( Stream.of( members).flatMap( Collection::stream ).collect( toList() ) );
    }

    public static class MemberInfo
    {
        private final String[] addresses;
        private final RoleInfo role;

        MemberInfo( String[] addresses, RoleInfo role )
        {
            this.addresses = addresses;
            this.role = role;
        }

        @Override
        public boolean equals( Object o )
        {
            if ( this == o )
            {
                return true;
            }
            if ( o == null || getClass() != o.getClass() )
            {
                return false;
            }
            MemberInfo that = (MemberInfo) o;
            return Arrays.equals( addresses, that.addresses ) && role == that.role;
        }

        @Override
        public int hashCode()
        {
            return Objects.hash( Arrays.hashCode( addresses ), role );
        }

        @Override
        public String toString()
        {
            return String.format( "MemberInfo{addresses='%s', role=%s}", Arrays.toString( addresses ), role );
        }
    }
}
