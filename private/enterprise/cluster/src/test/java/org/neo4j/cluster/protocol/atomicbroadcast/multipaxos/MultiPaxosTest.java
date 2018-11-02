/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cluster.protocol.atomicbroadcast.multipaxos;

import org.junit.Test;

import java.io.Serializable;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import org.neo4j.cluster.MultipleFailureLatencyStrategy;
import org.neo4j.cluster.NetworkMock;
import org.neo4j.cluster.ScriptableNetworkFailureLatencyStrategy;
import org.neo4j.cluster.TestProtocolServer;
import org.neo4j.cluster.protocol.atomicbroadcast.AtomicBroadcast;
import org.neo4j.cluster.protocol.atomicbroadcast.AtomicBroadcastSerializer;
import org.neo4j.cluster.protocol.atomicbroadcast.ObjectStreamFactory;
import org.neo4j.cluster.protocol.cluster.Cluster;
import org.neo4j.cluster.timeout.FixedTimeoutStrategy;
import org.neo4j.cluster.timeout.MessageTimeoutStrategy;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.logging.internal.NullLogService;

public class MultiPaxosTest
{
    @Test
    public void testFailure() throws Exception
    {
        ScriptableNetworkFailureLatencyStrategy networkLatency = new ScriptableNetworkFailureLatencyStrategy();
        NetworkMock network = new NetworkMock( NullLogService.getInstance(), new Monitors(), 50,
                new MultipleFailureLatencyStrategy( networkLatency ),
                new MessageTimeoutStrategy( new FixedTimeoutStrategy( 1000 ) ) );

        List<TestProtocolServer> nodes = new ArrayList<>();

        TestProtocolServer server = network.addServer( 1, URI.create( "cluster://server1" ) );
        server.newClient( Cluster.class ).create( "default" );
        network.tickUntilDone();
        nodes.add( server );

        for ( int i = 1; i < 3; i++ )
        {
            TestProtocolServer protocolServer = network.addServer( i + 1, new URI( "cluster://server" + (i + 1) ) );
            protocolServer.newClient( Cluster.class ).join( "default", new URI( "cluster://server1" ) );
            network.tick( 10 );
            nodes.add( protocolServer );
        }

        final AtomicBroadcast atomicBroadcast = nodes.get( 0 ).newClient( AtomicBroadcast.class );
        ObjectStreamFactory objectStreamFactory = new ObjectStreamFactory();
        final AtomicBroadcastSerializer serializer = new AtomicBroadcastSerializer( objectStreamFactory,
                objectStreamFactory );
        atomicBroadcast.broadcast( serializer.broadcast( new DaPayload() ) );

        networkLatency.nodeIsDown( "cluster://server2" );
        networkLatency.nodeIsDown( "cluster://server3" );

        atomicBroadcast.broadcast( serializer.broadcast( new DaPayload() ) );
        network.tick( 100 );
        networkLatency.nodeIsUp( "cluster://server3" );
        network.tick( 1000 );

        for ( TestProtocolServer node : nodes )
        {
            node.newClient( Cluster.class ).leave();
            network.tick( 10 );
        }

    }

    private static final class DaPayload implements Serializable
    {
        private static final long serialVersionUID = -2896543854010391900L;
    }
}
