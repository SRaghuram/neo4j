/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cluster.protocol.cluster;

import org.junit.Test;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;

import org.neo4j.cluster.InstanceId;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.logging.NullLogProvider;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.neo4j.test.mockito.matcher.IterableMatcher.matchesIterable;

public class ClusterConfigurationTest
{
    public static URI NEO4J_SERVER1_URI;
    public static InstanceId NEO4J_SERVER_ID;

    static
    {
        try
        {
            NEO4J_SERVER1_URI = new URI( "neo4j://server1" );
            NEO4J_SERVER_ID = new InstanceId( 1 );
        }
        catch ( URISyntaxException e )
        {
            e.printStackTrace();
        }
    }

    ClusterConfiguration configuration = new ClusterConfiguration( "default", NullLogProvider.getInstance(),
            new ArrayList<>() );

    @Test
    public void givenEmptyClusterWhenNodeAddedThenNodeWasAdded()
    {
        configuration.joined( NEO4J_SERVER_ID, NEO4J_SERVER1_URI );

        assertThat( configuration.getMemberIds(), matchesIterable( Iterables.iterable( NEO4J_SERVER_ID ) ) );
        assertThat( configuration.getUriForId( NEO4J_SERVER_ID ), equalTo( NEO4J_SERVER1_URI ) );
        assertThat( configuration.getMemberURIs(), equalTo( Arrays.asList( NEO4J_SERVER1_URI ) ) );
    }

    @Test
    public void givenEmptyClusterWhenNodeIsAddedTwiceThenNodeWasAddedOnce()
    {
        configuration.joined( NEO4J_SERVER_ID, NEO4J_SERVER1_URI );
        configuration.joined( NEO4J_SERVER_ID, NEO4J_SERVER1_URI );

        assertThat( configuration.getMemberIds(), matchesIterable( Iterables.iterable( NEO4J_SERVER_ID ) ) );
        assertThat( configuration.getUriForId( NEO4J_SERVER_ID ), equalTo( NEO4J_SERVER1_URI ) );
        assertThat( configuration.getMemberURIs(), equalTo( Arrays.asList( NEO4J_SERVER1_URI ) ) );
    }

    @Test
    public void givenClusterWithOneNodeWhenNodeIsRemovedThenClusterIsEmpty()
    {
        configuration.joined( NEO4J_SERVER_ID, NEO4J_SERVER1_URI );
        configuration.left( NEO4J_SERVER_ID );

        assertThat( configuration.getMemberIds(), matchesIterable( Iterables.empty() ) );
        assertThat( configuration.getUriForId( NEO4J_SERVER_ID ), equalTo( null ) );
        assertThat( configuration.getMemberURIs(), equalTo( Collections.<URI>emptyList() ) );

    }

    @Test
    public void givenClusterWithOneNodeWhenNodeIsRemovedTwiceThenClusterIsEmpty()
    {
        configuration.joined( NEO4J_SERVER_ID, NEO4J_SERVER1_URI );
        configuration.left( NEO4J_SERVER_ID );
        configuration.left( NEO4J_SERVER_ID );

        assertThat( configuration.getMemberIds(), matchesIterable( Iterables.empty() ) );
        assertThat( configuration.getUriForId( NEO4J_SERVER_ID ), equalTo( null ) );
        assertThat( configuration.getMemberURIs(), equalTo( Collections.<URI>emptyList() ) );

    }
}

