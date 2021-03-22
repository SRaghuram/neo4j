/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.enterprise;

import com.neo4j.causalclustering.common.Cluster;
import com.neo4j.causalclustering.common.ClusterMember;
import org.assertj.core.api.Condition;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.function.Function;

import static com.neo4j.server.enterprise.ClusteringEndpointHelpers.queryCombinedStatusEndpoint;
import static com.neo4j.server.enterprise.ClusteringEndpointHelpers.queryStatusEndpoint;
import static java.lang.String.format;
import static java.util.Collections.emptyMap;
import static java.util.stream.Collectors.toList;

class ClusteringStatusEndpointMatchers
{
    private static final String FIELD_THROUGHPUT = "raftCommandsPerSecond";
    private static final String FIELD_CORE = "core";
    private static final String FIELD_MEMBER = "memberId";
    private static final String FIELD_LAST_INDEX = "lastAppliedRaftIndex";
    private static final String FIELD_HEALTHY = "healthy";
    private static final String DISCOVERY_FIELD_HEALTHY = "discoveryHealthy";
    private static final String FIELD_LEADER = "leader";
    private static final String FIELD_VOTING = "votingMembers";
    private static final String FIELD_PARTICIPATING = "participatingInRaftGroup";
    private static final String FIELD_LAST_MESSAGE = "millisSinceLastLeaderMessage";

    private static final String FIELD_DATABASE_NAME = "databaseName";
    private static final String FIELD_DATABASE_UUID = "databaseUuid";
    private static final String FIELD_DATABASE_STATUS = "databaseStatus";

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

        static Matcher<Map<String,Object>> discoveryHealthFieldIs( Matcher<Boolean> matcher )
        {
            return new StatusDescriptionFieldMatcher<>( o -> Boolean.parseBoolean( o.toString() ), DISCOVERY_FIELD_HEALTHY, matcher );
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

    static <T> Condition<Collection<T>> allValuesEqual()
    {
        return new Condition<>( values -> values.stream().distinct().count() == 1, "Values should be equal" );
    }

    static Callable<Map<String,Object>> statusEndpoint( ClusterMember server, String databaseName )
    {
        return () -> queryStatusEndpoint( server, databaseName ).body();
    }

    @SuppressWarnings( "unchecked" )
    static Callable<Map<String,Object>> statusFromCombinedEndpoint( ClusterMember server, String databaseName, UUID databaseUuid )
    {
        return () ->
                combinedStatusEndpoint( server ).call()
                                                .stream()
                                                .filter( element -> databaseName.equals( element.get( FIELD_DATABASE_NAME ) ) )
                                                .filter( element -> databaseUuid.toString().equals( element.get( FIELD_DATABASE_UUID ) ) )
                                                .map( element -> (Map<String,Object>) element.get( FIELD_DATABASE_STATUS ) )
                                                .findFirst()
                                                .orElse( emptyMap() );
    }

    static Callable<List<Map<String,Object>>> combinedStatusEndpoint( ClusterMember server )
    {
        return () -> queryCombinedStatusEndpoint( server ).body();
    }

    static Callable<Boolean> canVote( Callable<Map<String,Object>> statusDescription )
    {
        return () -> Boolean.parseBoolean( statusDescription.call().get( FIELD_PARTICIPATING ).toString() );
    }

    static Callable<Boolean> databaseHealthy( Callable<Map<String,Object>> statusDescription )
    {
        return () -> Boolean.parseBoolean( statusDescription.call().get( FIELD_HEALTHY ).toString() );
    }

    static <T> Callable<Collection<T>> asCollection( Callable<T> supplier )
    {
        return () -> Collections.singletonList( supplier.call() );
    }

    static <T extends Exception> Callable<Collection<Long>> lastAppliedRaftIndex( Callable<Collection<Map<String,Object>>> statusSupplier )
    {
        return () -> statusSupplier.call()
                .stream()
                .map( status -> status.get( FIELD_LAST_INDEX ).toString() )
                .map( Long::parseLong )
                .collect( toList() );
    }

    static Callable<Collection<Map<String,Object>>> allStatusEndpointValues( Cluster cluster, String databaseName )
    {
        return () -> cluster.allMembers()
                .stream()
                .map( controls -> queryStatusEndpoint( controls, databaseName ).body() )
                .collect( toList() );
    }

    static <T> Callable<Collection<T>> allReplicaFieldValues( Cluster cluster, Function<ClusterMember,T> mapper )
    {
        return () -> cluster.readReplicas().stream().map( mapper ).collect( toList() );
    }
}
