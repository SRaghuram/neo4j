/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.scenarios;

import com.neo4j.causalclustering.net.Server;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.connectors.ConnectorPortRegister;
import org.neo4j.helpers.ListenSocketAddress;
import org.neo4j.logging.AssertableLogProvider;

import static com.neo4j.causalclustering.net.BootstrapConfiguration.serverConfig;

class ConnectionInfoIT
{
    private Socket testSocket;

    @AfterEach
    void teardown() throws IOException
    {
        if ( testSocket != null )
        {
            unbind( testSocket );
        }
    }

    @Test
    void testAddressAlreadyBoundMessage() throws Throwable
    {
        // given
        testSocket = bindPort( "localhost", 0 );

        // when
        AssertableLogProvider logProvider = new AssertableLogProvider();
        AssertableLogProvider userLogProvider = new AssertableLogProvider();
        ListenSocketAddress listenSocketAddress = new ListenSocketAddress( "localhost", testSocket.getLocalPort() );

        ExecutorService executor = Executors.newCachedThreadPool();
        Server catchupServer = new Server( channel -> { }, null, logProvider, userLogProvider, listenSocketAddress, "server-name", executor,
                new ConnectorPortRegister(), serverConfig( Config.defaults() ) );

        //then
        try
        {
            catchupServer.start();
        }
        catch ( Throwable throwable )
        {
            //expected.
        }
        finally
        {
            executor.shutdown();
        }
        String expectedPartOfMessage = String.format( "server-name: cannot bind to '%s' with transport ", listenSocketAddress );
        logProvider.assertContainsMessageContaining( expectedPartOfMessage );
        userLogProvider.assertContainsMessageContaining( expectedPartOfMessage );
    }

    @SuppressWarnings( "SameParameterValue" )
    private Socket bindPort( String address, int port ) throws IOException
    {
        Socket socket = new Socket();
        socket.bind( new InetSocketAddress( address, port ) );
        return socket;
    }

    private void unbind( Socket socket ) throws IOException
    {
        socket.close();
    }
}
