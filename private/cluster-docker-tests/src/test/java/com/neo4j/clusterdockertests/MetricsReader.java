/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.clusterdockertests;

import org.assertj.core.api.Assert;
import org.junit.platform.commons.logging.Logger;
import org.junit.platform.commons.logging.LoggerFactory;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;

import org.neo4j.driver.Driver;
import org.neo4j.driver.Value;
import org.neo4j.driver.Values;
import org.neo4j.internal.helpers.collection.Pair;

import static org.assertj.core.api.Assertions.assertThat;

public class MetricsReader
{
    public MetricsReader()
    {

    }

    public void checkExpectations( Driver driver, MetricExpectations expectations )
    {
        var keysWeNeedToCheck = expectations.getKeys();
        Set<String> keysThatHaveBeenChecked = new HashSet<>( keysWeNeedToCheck.size() );
        try ( var session = driver.session() )
        {
            session.run( "CALL dbms.queryJmx($pattern)", Map.of( "pattern", "neo4j.metrics:*" ) )
                   .stream()
                   .forEach( r ->
                             {
                                 var name = r.get( "name" ).asString();
                                 var attributes = r.get( "attributes" ).asMap( Values.ofValue() );
                                 if ( expectations.containsKey( name ) )
                                 {
                                     expectations.check( name, attributes );
                                     keysThatHaveBeenChecked.add( name );
                                 }
                             }
                   );
        }
        assertThat( keysThatHaveBeenChecked ).containsExactlyInAnyOrderElementsOf( keysWeNeedToCheck );
    }

    @FunctionalInterface
    public interface MetricParser<T>
    {
        MetricParser<Long> ClusterReplicatedData = m -> m.get( "Value" ).asMap( Values.ofValue() ).get( "value" ).asLong();

        T parse( Map<String,Value> attributes );
    }

    public static Pair<String,MetricParser<Long>> replicatedDataMetric( String name )
    {
        return Pair.of( "neo4j.metrics:name=neo4j.causal_clustering.core.discovery.replicated_data." + name, MetricParser.ClusterReplicatedData );
    }

    public static class MetricExpectations
    {

        private final Logger log = LoggerFactory.getLogger( this.getClass() );
        private final Map<String,Consumer<Map<String,Value>>> expectations = new HashMap<>();

        <T> MetricExpectations add( Pair<String,MetricParser<T>> metricDefinition, Function<T,Assert<?,T>> assertion )
        {
            return add( metricDefinition.first(), metricDefinition.other(), assertion );
        }

        <T> MetricExpectations add( String key, MetricParser<T> parser, Function<T,Assert<?,T>> assertion )
        {
            assertThat( expectations ).as( "cannot set multiple expectations on a single metric" ).doesNotContainKey( key );
            expectations.put( key, a -> assertion.apply( parser.parse( a ) ) );
            return this;
        }

        boolean containsKey( String key )
        {
            return expectations.containsKey( key );
        }

        void assertMeetsExpectations( String key, Map<String,Value> attributes )
        {
            expectations.get( key ).accept( attributes );
        }

        void check( String key, Map<String,Value> attributes )
        {
            log.info( () -> String.format( "Checking metric: %s %s", key, attributes ) );
            assertMeetsExpectations( key, attributes );
        }

        public Set<String> getKeys()
        {
            return expectations.keySet();
        }
    }

    public static Function<Long,Assert<?,Long>> isLessThanOrEqualTo( int expected )
    {
        return m -> assertThat( m ).isLessThanOrEqualTo( expected );
    }

    public static Function<Long,Assert<?,Long>> isEqualTo( int expected )
    {
        return m -> assertThat( m ).isEqualTo( expected );
    }
}
