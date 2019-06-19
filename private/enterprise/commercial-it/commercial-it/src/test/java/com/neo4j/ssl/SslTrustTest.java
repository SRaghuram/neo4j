/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.ssl;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;

import java.util.concurrent.ExecutionException;
import javax.net.ssl.SSLException;

import org.neo4j.ssl.SslResource;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.test.rule.fs.DefaultFileSystemRule;

import static com.neo4j.ssl.SslContextFactory.makeSslPolicy;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.neo4j.ssl.SslResourceBuilder.caSignedKeyId;
import static org.neo4j.ssl.SslResourceBuilder.selfSignedKeyId;

public class SslTrustTest
{
    private static final int UNRELATED_ID = 5; // SslContextFactory requires us to trust something

    private static final byte[] REQUEST = {1, 2, 3, 4};

    @Rule
    public TestDirectory testDir = TestDirectory.testDirectory();

    @Rule
    public DefaultFileSystemRule fsRule = new DefaultFileSystemRule();

    private SecureServer server;
    private SecureClient client;
    private ByteBuf expected;

    @After
    public void cleanup()
    {
        if ( expected != null )
        {
            expected.release();
        }
        if ( client != null )
        {
            client.disconnect();
        }
        if ( server != null )
        {
            server.stop();
        }
    }

    @Test
    public void partiesWithMutualTrustShouldCommunicate() throws Exception
    {
        // given
        SslResource sslServerResource = selfSignedKeyId( 0 ).trustKeyId( 1 ).install( testDir.directory( "server" ) );
        SslResource sslClientResource = selfSignedKeyId( 1 ).trustKeyId( 0 ).install( testDir.directory( "client" ) );

        server = new SecureServer( makeSslPolicy( sslServerResource ) );

        server.start();
        client = new SecureClient( makeSslPolicy( sslClientResource ) );
        client.connect( server.port() );

        // when
        ByteBuf request = ByteBufAllocator.DEFAULT.buffer().writeBytes( REQUEST );
        client.channel().writeAndFlush( request );

        // then
        expected = ByteBufAllocator.DEFAULT.buffer().writeBytes( SecureServer.RESPONSE );
        client.sslHandshakeFuture().get( 1, MINUTES );
        client.assertResponse( expected );
    }

    @Test
    public void partiesWithMutualTrustThroughCAShouldCommunicate() throws Exception
    {
        // given
        SslResource sslServerResource = caSignedKeyId( 0 ).trustSignedByCA().install( testDir.directory( "server" ) );
        SslResource sslClientResource = caSignedKeyId( 1 ).trustSignedByCA().install( testDir.directory( "client" ) );

        server = new SecureServer( makeSslPolicy( sslServerResource ) );

        server.start();
        client = new SecureClient( makeSslPolicy( sslClientResource ) );
        client.connect( server.port() );

        // when
        ByteBuf request = ByteBufAllocator.DEFAULT.buffer().writeBytes( REQUEST );
        client.channel().writeAndFlush( request );

        // then
        expected = ByteBufAllocator.DEFAULT.buffer().writeBytes( SecureServer.RESPONSE );
        client.sslHandshakeFuture().get( 1, MINUTES );
        client.assertResponse( expected );
    }

    @Test
    public void serverShouldNotCommunicateWithUntrustedClient() throws Exception
    {
        // given
        SslResource sslClientResource = selfSignedKeyId( 1 ).trustKeyId( 0 ).install( testDir.directory( "client" ) );
        SslResource sslServerResource = selfSignedKeyId( 0 ).trustKeyId( UNRELATED_ID ).install( testDir.directory( "server" ) );

        server = new SecureServer( makeSslPolicy( sslServerResource ) );

        server.start();
        client = new SecureClient( makeSslPolicy( sslClientResource ) );
        client.connect( server.port() );

        try
        {
            // when
            client.sslHandshakeFuture().get( 1, MINUTES );
            fail();
        }
        catch ( ExecutionException e )
        {
            assertThat( e.getCause(), instanceOf( SSLException.class ) );
        }
    }

    @Test
    public void clientShouldNotCommunicateWithUntrustedServer() throws Exception
    {
        // given
        SslResource sslClientResource = selfSignedKeyId( 0 ).trustKeyId( UNRELATED_ID ).install( testDir.directory( "client" ) );
        SslResource sslServerResource = selfSignedKeyId( 1 ).trustKeyId( 0 ).install( testDir.directory( "server" ) );

        server = new SecureServer( makeSslPolicy( sslServerResource ) );

        server.start();
        client = new SecureClient( makeSslPolicy( sslClientResource ) );
        client.connect( server.port() );

        try
        {
            client.sslHandshakeFuture().get( 1, MINUTES );
            fail();
        }
        catch ( ExecutionException e )
        {
            assertThat( e.getCause(), instanceOf( SSLException.class ) );
        }
    }

    @Test
    public void partiesWithMutualTrustThroughCAShouldNotCommunicateWhenServerRevoked() throws Exception
    {
        // given
        SslResource sslServerResource = caSignedKeyId( 0 ).trustSignedByCA().install( testDir.directory( "server" ) );
        SslResource sslClientResource = caSignedKeyId( 1 ).trustSignedByCA().revoke( 0 ).install( testDir.directory( "client" ) );

        server = new SecureServer( makeSslPolicy( sslServerResource ) );

        server.start();
        client = new SecureClient( makeSslPolicy( sslClientResource ) );
        client.connect( server.port() );

        try
        {
            client.sslHandshakeFuture().get( 1, MINUTES );
            fail( "Server should have been revoked" );
        }
        catch ( ExecutionException e )
        {
            assertThat( e.getCause(), instanceOf( SSLException.class ) );
        }
    }

    @Test
    public void partiesWithMutualTrustThroughCAShouldNotCommunicateWhenClientRevoked() throws Exception
    {
        // given
        SslResource sslServerResource = caSignedKeyId( 0 ).trustSignedByCA().revoke( 1 ).install( testDir.directory( "server" ) );
        SslResource sslClientResource = caSignedKeyId( 1 ).trustSignedByCA().install( testDir.directory( "client" ) );

        server = new SecureServer( makeSslPolicy( sslServerResource ) );

        server.start();
        client = new SecureClient( makeSslPolicy( sslClientResource ) );
        client.connect( server.port() );

        try
        {
            client.sslHandshakeFuture().get( 1, MINUTES );
            fail( "Client should have been revoked" );
        }
        catch ( ExecutionException e )
        {
            assertThat( e.getCause(), instanceOf( SSLException.class ) );
        }
    }
}
