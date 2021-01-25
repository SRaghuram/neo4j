/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.net;

import com.neo4j.causalclustering.identity.RaftGroupId;
import com.neo4j.causalclustering.net.LoadBalancedTrackingChannelPoolMap.RaftGroupSocket;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.pool.ChannelPool;
import io.netty.channel.pool.ChannelPoolHandler;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.UUID;
import java.util.concurrent.ExecutionException;

import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.kernel.database.DatabaseIdFactory;

import static org.assertj.core.api.Assertions.assertThat;

class LoadBalancedTrackingChannelPoolMapTest
{

    private LoadBalancedTrackingChannelPoolMap map;

    @BeforeEach
    void setUp()
    {
        map = new LoadBalancedTrackingChannelPoolMap( new Bootstrap(), null, new MockedChannelPoolFactory(), 3 );
    }

    @Test
    void returnSameChannelForSameKey() throws ExecutionException, InterruptedException
    {
        // given:
        var groupId = RaftGroupId.from( DatabaseIdFactory.from( UUID.randomUUID() ) );
        var addr = new SocketAddress( 1337 );
        var key = new RaftGroupSocket( groupId, addr );

        // when:
        var fstChannelPool = map.get( key );
        var sndChannelPool = map.get( key );

        // then:
        assertThat( fstChannelPool ).isSameAs( sndChannelPool );
    }

    @Test
    void returnDifferentChannelForDifferentKey() throws ExecutionException, InterruptedException
    {
        // given:
        var groupId1 = RaftGroupId.from( DatabaseIdFactory.from( UUID.randomUUID() ) );
        var groupId2 = RaftGroupId.from( DatabaseIdFactory.from( UUID.randomUUID() ) );
        var addr = new SocketAddress( 1337 );
        var key1 = new RaftGroupSocket( groupId1, addr );
        var key2 = new RaftGroupSocket( groupId2, addr );

        // when:
        var fstChannelPool = map.get( key1 );
        var sndChannelPool = map.get( key2 );

        // then:
        assertThat( fstChannelPool ).isNotSameAs( sndChannelPool );
    }

    @Test
    void returnSameChannelForAllKeysWhenRestrictedToOneChannel() throws ExecutionException, InterruptedException
    {
        // given:
        var oneChannelMap = new LoadBalancedTrackingChannelPoolMap( new Bootstrap(), null, new MockedChannelPoolFactory(), 1 );
        var groupId1 = RaftGroupId.from( DatabaseIdFactory.from( UUID.randomUUID() ) );
        var groupId2 = RaftGroupId.from( DatabaseIdFactory.from( UUID.randomUUID() ) );
        var addr = new SocketAddress( 1337 );
        var key1 = new RaftGroupSocket( groupId1, addr );
        var key2 = new RaftGroupSocket( groupId2, addr );

        // when:
        var fstChannelPool = oneChannelMap.get( key1 );
        var sndChannelPool = oneChannelMap.get( key2 );

        // then:
        assertThat( fstChannelPool ).isSameAs( sndChannelPool );
    }

    @Test
    void canCreateMaxChannelsPerEachSocketAddress() throws ExecutionException, InterruptedException
    {
        // given:
        var oneChannelMap = new LoadBalancedTrackingChannelPoolMap( new Bootstrap(), null, new MockedChannelPoolFactory(), 1 );
        var groupId1 = RaftGroupId.from( DatabaseIdFactory.from( UUID.randomUUID() ) );
        var groupId2 = RaftGroupId.from( DatabaseIdFactory.from( UUID.randomUUID() ) );
        var addr = new SocketAddress( 1337 );
        var addr2 = new SocketAddress( 1338 );
        var key1 = new RaftGroupSocket( groupId1, addr );
        var key2 = new RaftGroupSocket( groupId2, addr2 );

        // when:
        var fstChannelPool = oneChannelMap.get( key1 );
        var sndChannelPool = oneChannelMap.get( key2 );

        // then:
        assertThat( fstChannelPool ).isNotSameAs( sndChannelPool );
    }

    private static class MockedChannelPoolFactory implements ChannelPoolFactory
    {
        @Override
        public ChannelPool create( Bootstrap bootstrap, ChannelPoolHandler handler )
        {

            return Mockito.mock( ChannelPool.class );
        }
    }
}
