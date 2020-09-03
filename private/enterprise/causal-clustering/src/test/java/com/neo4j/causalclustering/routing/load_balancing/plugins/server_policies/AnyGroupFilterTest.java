/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.routing.load_balancing.plugins.server_policies;

import com.neo4j.causalclustering.identity.IdFactory;
import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.Set;

import org.neo4j.configuration.helpers.SocketAddress;

import static com.neo4j.configuration.ServerGroupName.setOf;
import static org.junit.jupiter.api.Assertions.assertEquals;

class AnyGroupFilterTest
{
    @Test
    void shouldReturnServersMatchingAnyGroup()
    {
        // given
        AnyGroupFilter groupFilter = new AnyGroupFilter( setOf( "china-west", "europe" ) );

        ServerInfo serverA = new ServerInfo(
                new SocketAddress( "bolt", 1 ),
                IdFactory.randomServerId(),
                setOf( "china-west" )
        );
        ServerInfo serverB = new ServerInfo(
                new SocketAddress( "bolt", 2 ),
                IdFactory.randomServerId(),
                setOf( "europe" )
        );
        ServerInfo serverC = new ServerInfo(
                new SocketAddress( "bolt", 3 ),
                IdFactory.randomServerId(),
                setOf( "china", "china-west" )
        );
        ServerInfo serverD = new ServerInfo(
                new SocketAddress( "bolt", 4 ),
                IdFactory.randomServerId(),
                setOf( "china-west", "china" )
        );
        ServerInfo serverE = new ServerInfo(
                new SocketAddress( "bolt", 5 ),
                IdFactory.randomServerId(),
                setOf( "china-east", "asia" )
        );
        ServerInfo serverF = new ServerInfo(
                new SocketAddress( "bolt", 6 ),
                IdFactory.randomServerId(),
                setOf( "europe-west" )
        );
        ServerInfo serverG = new ServerInfo(
                new SocketAddress( "bolt", 7 ),
                IdFactory.randomServerId(),
                setOf( "china-west", "europe" )
        );
        ServerInfo serverH = new ServerInfo(
                new SocketAddress( "bolt", 8 ),
                IdFactory.randomServerId(),
                setOf( "africa" )
        );

        Set<ServerInfo> data = Set.of( serverA, serverB, serverC, serverD, serverE, serverF, serverG, serverH );

        // when
        Set<ServerInfo> output = groupFilter.apply( data );

        // then
        Set<Integer> ports = new HashSet<>();
        for ( ServerInfo info : output )
        {
            ports.add( info.boltAddress().getPort() );
        }

        assertEquals( Set.of( 1, 2, 3, 4, 7 ), ports );
    }
}
