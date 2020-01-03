/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.enterprise;

import com.neo4j.harness.internal.CausalClusterInProcessBuilder.CausalCluster;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import org.neo4j.function.ThrowingSupplier;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.harness.Neo4j;

import static com.neo4j.server.enterprise.CausalClusterRestEndpointHelpers.queryStatusEndpoint;
import static java.lang.String.format;
import static java.util.stream.Collectors.toList;

class CausalClusterStatusEndpointMatchers
{
    private static final String FIELD_THROUGHPUT = "raftCommandsPerSecond";
    private static final String FIELD_CORE = "core";
    private static final String FIELD_MEMBER = "memberId";
    private static final String FIELD_LAST_INDEX = "lastAppliedRaftIndex";
    private static final String FIELD_HEALTHY = "healthy";
    private static final String FIELD_LEADER = "leader";
    private static final String FIELD_VOTING = "votingMembers";
    private static final String FIELD_PARTICIPATING = "participatingInRaftGroup";
    private static final String FIELD_LAST_MESSAGE = "millisSinceLastLeaderMessage";

    static class FieldMatchers
    {
        static Matcher<Map<String,Object>> coreFieldIs( Matcher<Boolean> matcher )
        {
            return new StatusDescriptionFieldMatcher<>( o -> Boolean.parseBoolean( o.toString() ), FIELD_CORE, matcher );
        }

        static Matcher<Map<String,Object>> lastAppliedRaftIndexFieldIs( Matcher<Long> matcher )
        {
            return new StatusDescriptionFieldMatcher<>( o -> Long.parseLong( o.toString() ), FIELD_LAST_INDEX, matcher );
        }

        static Matcher<Map<String,Object>> memberIdFieldIs( Matcher<String> matcher )
        {
            return new StatusDescriptionFieldMatcher<>( Object::toString, FIELD_MEMBER, matcher );
        }

        static Matcher<Map<String,Object>> healthFieldIs( Matcher<Boolean> matcher )
        {
            return new StatusDescriptionFieldMatcher<>( o -> Boolean.parseBoolean( o.toString() ), FIELD_HEALTHY, matcher );
        }

        static Matcher<Map<String,Object>> leaderFieldIs( Matcher<String> matcher )
        {
            return new StatusDescriptionFieldMatcher<>( Object::toString, FIELD_LEADER, matcher );
        }

        static Matcher<Map<String,Object>> raftMessageThroughputPerSecondFieldIs( Matcher<Double> matcher )
        {
            return new StatusDescriptionFieldMatcher<>( o -> Double.parseDouble( o.toString() ), FIELD_THROUGHPUT, matcher );
        }

        /**
         * Matcher for voting membership set from the kv map of the status description endpoint response
         *
         * @param matcher matching condition on the list of values from the voting membership set. Can be cast to {@code (List<String>)}.
         * @return a matcher for the voting membership set for the kv Map of the status description endpoint
         */
        static Matcher<Map<String,Object>> votingMemberSetIs( Matcher<Collection<?>> matcher )
        {
            return new StatusDescriptionFieldMatcher<>( obj -> (List<String>) obj, FIELD_VOTING, matcher );
        }

        static Matcher<Map<String,Object>> participatingInRaftGroup( boolean isCore )
        {
            return new TypeSafeMatcher<>()
            {
                @Override
                protected boolean matchesSafely( Map<String,Object> item )
                {
                    Object raw = item.get( FIELD_PARTICIPATING );
                    if ( raw == null )
                    {
                        return false;
                    }
                    if ( isCore )
                    {
                        return Boolean.parseBoolean( raw.toString() );
                    }
                    else
                    {
                        return !Boolean.parseBoolean( raw.toString() );
                    }
                }

                @Override
                public void describeTo( Description description )
                {
                    description.appendText( format( "Field `%s` should be `%b`", FIELD_PARTICIPATING, isCore ) );
                }
            };
        }

        static Matcher<Map<String,Object>> millisSinceLastLeaderMessageSanityCheck( boolean isCore )
        {

            return new TypeSafeMatcher<>()
            {
                @Override
                protected boolean matchesSafely( Map<String,Object> item )
                {
                    boolean isLeader = item.get( FIELD_LEADER ).equals( item.get( FIELD_MEMBER ) );
                    Object millisSinceLastLeaderMessage = item.get( FIELD_LAST_MESSAGE );
                    if ( isCore && isLeader )
                    {
                        return Long.parseLong( millisSinceLastLeaderMessage.toString() ) == 0L;
                    }
                    else if ( isCore )
                    {
                        return Long.parseLong( millisSinceLastLeaderMessage.toString() ) > 0L;
                    }
                    else
                    {
                        return millisSinceLastLeaderMessage == null;
                    }
                }

                @Override
                public void describeTo( Description description )
                {
                    if ( isCore )
                    {
                        description.appendText( format( "Field `%s` should be 0 if leader or great than 0 if not leader", FIELD_LAST_MESSAGE ) );
                    }
                    else
                    {
                        description.appendText( format( "Field `%s` should be null", FIELD_LAST_MESSAGE ) );
                    }
                }
            };
        }
    }

    static <T> Matcher<Collection<T>> allValuesEqual()
    {
        return new TypeSafeMatcher<>()
        {
            @Override
            public boolean matchesSafely( Collection<T> item )
            {
                return item.stream().distinct().count() == 1;
            }

            @Override
            public void describeTo( Description description )
            {
                description.appendText( "Values should be equal" );
            }
        };
    }

    static ThrowingSupplier<Map<String,Object>,RuntimeException> statusEndpoint( Neo4j server, String databaseName )
    {
        return () -> queryStatusEndpoint( server, databaseName ).body();
    }

    static ThrowingSupplier<Boolean,RuntimeException> canVote( ThrowingSupplier<Map<String,Object>,RuntimeException> statusDescription )
    {
        return () -> Boolean.parseBoolean( statusDescription.get().get( FIELD_PARTICIPATING ).toString() );
    }

    static Long getNodeCount( Neo4j serverControls )
    {
        GraphDatabaseService db = serverControls.defaultDatabaseService();
        try ( Transaction transaction = db.beginTx() )
        {
            return transaction.getAllNodes().stream().count();
        }
    }

    static <T> ThrowingSupplier<Collection<T>,RuntimeException> asCollection( ThrowingSupplier<T,RuntimeException> supplier )
    {
        return () -> Collections.singletonList( supplier.get() );
    }

    static <T extends Exception> ThrowingSupplier<Collection<Long>,T> lastAppliedRaftIndex( ThrowingSupplier<Collection<Map<String,Object>>,T> statusSupplier )
    {
        return () -> statusSupplier.get()
                .stream()
                .map( status -> status.get( FIELD_LAST_INDEX ).toString() )
                .map( Long::parseLong )
                .collect( toList() );
    }

    static ThrowingSupplier<Collection<Map<String,Object>>,RuntimeException> allStatusEndpointValues( CausalCluster cluster, String databaseName )
    {
        return () -> cluster.getCoresAndReadReplicas()
                .stream()
                .map( controls -> queryStatusEndpoint( controls, databaseName ).body() )
                .collect( toList() );
    }

    static <T> ThrowingSupplier<Collection<T>,RuntimeException> allReplicaFieldValues( CausalCluster cluster,
            Function<Neo4j,T> mapper )
    {
        return () -> cluster.getReadReplicas().stream().map( mapper ).collect( toList() );
    }
}
