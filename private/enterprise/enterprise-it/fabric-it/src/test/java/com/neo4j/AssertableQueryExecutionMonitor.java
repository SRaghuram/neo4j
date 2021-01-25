/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j;

import org.apache.commons.lang3.builder.ReflectionToStringBuilder;
import org.apache.commons.lang3.tuple.Triple;
import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.Matchers;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

import org.neo4j.kernel.api.query.ExecutingQuery;
import org.neo4j.kernel.api.query.QuerySnapshot;
import org.neo4j.kernel.impl.query.QueryExecutionMonitor;

public class AssertableQueryExecutionMonitor
{
    enum EventType
    {
        StartProcessing,
        StartExecution,
        EndFailure,
        EndSuccess
    }

    static class Event
    {
        final EventType type;
        final ExecutingQuery query;
        final Throwable failure;
        final QuerySnapshot snapshot;
        final String failureMessage;

        Event( EventType type, ExecutingQuery query, QuerySnapshot snapshot, Throwable failure, String failureMessage )
        {
            this.type = type;
            this.query = query;
            this.snapshot = snapshot;
            this.failure = failure;
            this.failureMessage = failureMessage;
        }

        @Override
        public String toString()
        {
            return ReflectionToStringBuilder.toString( this );
        }
    }

    public static CompositeMatcher<ExecutingQuery> query()
    {
        return new CompositeMatcher<>( ExecutingQuery.class );
    }

    public static CompositeMatcher<Throwable> throwable( Matcher<Class<? extends Throwable>> clazz, Matcher<String> message )
    {
        return new CompositeMatcher<>( Throwable.class )
                .where( "class", Throwable::getClass, clazz )
                .where( "message", Throwable::getMessage, message );
    }

    public static CompositeMatcher<Event> startProcessing()
    {
        return new CompositeMatcher<>( Event.class )
                .where( "type", e -> e.type, Matchers.is( EventType.StartProcessing ) );
    }

    public static CompositeMatcher<Event> startExecution()
    {
        return new CompositeMatcher<>( Event.class )
                .where( "type", e -> e.type, Matchers.is( EventType.StartExecution ) );
    }

    public static CompositeMatcher<Event> endSuccess()
    {
        return new CompositeMatcher<>( Event.class )
                .where( "type", e -> e.type, Matchers.is( EventType.EndSuccess ) );
    }

    public static CompositeMatcher<Event> endFailure()
    {
        return new CompositeMatcher<>( Event.class )
                .where( "type", e -> e.type, Matchers.is( EventType.EndFailure ) );
    }

    public static class CompositeMatcher<T> extends BaseMatcher<T>
    {
        private final Class<T> clazz;
        private final List<Triple<String,Function<T,?>,Matcher<?>>> matchers = new ArrayList<>();

        CompositeMatcher( Class<T> clazz )
        {
            this.clazz = clazz;
        }

        @Override
        public boolean matches( Object actual )
        {
            if ( actual != null && clazz.isAssignableFrom( actual.getClass() ) )
            {
                return matchers.stream().allMatch( m ->
                {
                    var matcher = m.getRight();
                    var extractor = m.getMiddle();
                    //noinspection unchecked
                    return matcher.matches( extractor.apply( (T) actual ) );
                } );
            }
            else
            {
                return false;
            }
        }

        @Override
        public void describeTo( Description description )
        {
            description.appendText( clazz.getSimpleName() + " where" );
            matchers.forEach( m -> description
                    .appendText( " " + m.getLeft() + ": " )
                    .appendDescriptionOf( m.getRight() )
                    .appendText( ", " )
            );
        }

        public <U> CompositeMatcher<T> where( String name, Function<T,U> extractor, Matcher<U> matcher )
        {
            matchers.add( Triple.of( name, extractor, matcher ) );
            return this;
        }
    }

    public static class Monitor implements QueryExecutionMonitor
    {
        public final List<Event> events = new ArrayList<>();

        @Override
        public void startProcessing( ExecutingQuery query )
        {
            events.add( new Event( EventType.StartProcessing, query, query.snapshot(), null, null ) );
        }

        @Override
        public void startExecution( ExecutingQuery query )
        {
            events.add( new Event( EventType.StartExecution, query, query.snapshot(), null, null ) );
        }

        @Override
        public void endFailure( ExecutingQuery query, Throwable failure )
        {
            events.add( new Event( EventType.EndFailure, query, query.snapshot(), failure, failure.getMessage() ) );
        }

        @Override
        public void endFailure( ExecutingQuery query, String reason )
        {
            events.add( new Event( EventType.EndFailure, query, query.snapshot(), null, reason ) );
        }

        @Override
        public void endSuccess( ExecutingQuery query )
        {
            events.add( new Event( EventType.EndSuccess, query, query.snapshot(), null, null ) );
        }
    }
}
