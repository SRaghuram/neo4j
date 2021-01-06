/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.model.model;

import com.neo4j.bench.model.util.JsonUtil;
import org.junit.jupiter.api.Test;

import java.util.Set;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class EnvironmentTest
{

    @Test
    public void environmentFromInstances()
    {
        // given
        Instance instance = new Instance( "hostname",
                                          Instance.Kind.AWS,
                                          "Linux",
                                          8,
                                          16384 );
        // when
        Environment environment = Environment.from( instance );

        // tehn
        Set<Instance> distinctInstances = environment.distinctInstances();
        assertThat( distinctInstances, contains( equalTo( instance ) ) );
        assertEquals( 1, environment.instances().get( instance ).intValue() );
    }

    @Test
    public void environmentFromTwoSameInstances()
    {
        // given
        Instance instance = new Instance( "hostname",
                                          Instance.Kind.AWS,
                                          "Linux",
                                          8,
                                          16384 );
        // when
        Environment environment = Environment.from( instance, instance );

        // tehn
        Set<Instance> distinctInstances = environment.distinctInstances();
        assertThat( distinctInstances, contains( equalTo( instance ) ) );
        assertEquals( 2, environment.instances().get( instance ).intValue() );
    }

    @Test
    public void environmentFromTwoDifferentInstances()
    {
        // given
        Instance instance0 = new Instance( "hostname0",
                                           Instance.Kind.AWS,
                                           "Linux",
                                           8,
                                           16384 );

        Instance instance1 = new Instance( "hostname1",
                                           Instance.Kind.AWS,
                                           "Linux",
                                           8,
                                           16384 );
        // when
        Environment environment = Environment.from( instance0, instance1 );

        // tehn
        Set<Instance> distinctInstances = environment.distinctInstances();
        assertThat( distinctInstances, containsInAnyOrder( equalTo( instance0 ), equalTo( instance1 ) ) );
        assertEquals( 1, environment.instances().get( instance0 ).intValue() );
        assertEquals( 1, environment.instances().get( instance1 ).intValue() );
    }

    @Test
    public void serializeLocalEnvironment()
    {
        Environment environment = Environment.local();
        Environment actualEnvironment = JsonUtil.deserializeJson( JsonUtil.serializeJson( environment ), Environment.class );
        assertEquals( environment, actualEnvironment );
    }
}
