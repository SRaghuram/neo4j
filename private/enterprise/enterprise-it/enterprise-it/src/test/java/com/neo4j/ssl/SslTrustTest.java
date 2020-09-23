/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.ssl;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.ExecutionException;
import javax.net.ssl.SSLException;

import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.ssl.SslResource;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static com.neo4j.ssl.SslContextFactory.makeSslPolicy;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.configuration.ssl.SslPolicyScope.TESTING;
import static org.neo4j.ssl.SslResourceBuilder.caSignedKeyId;
import static org.neo4j.ssl.SslResourceBuilder.selfSignedKeyId;

@TestDirectoryExtension
class SslTrustTest
{
    private static final int UNRELATED_ID = 5; // SslContextFactory requires us to trust something
    private static final byte[] REQUEST = {1, 2, 3, 4};

    @Inject
    private TestDirectory testDir;
    @Inject
    private DefaultFileSystemAbstraction fs;

    private SecureServer server;
    private SecureClient client;
    private ByteBuf expected;

    @AfterEach
    void cleanup()
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
    void partiesWithMutualTrustShouldCommunicate() throws Exception
    {
        // given
        SslResource sslServerResource = selfSignedKeyId( 0 ).trustKeyId( 1 ).install( testDir.directory( "server" ) );
        SslResource sslClientResource = selfSignedKeyId( 1 ).trustKeyId( 0 ).install( testDir.directory( "client" ) );

        server = new SecureServer( makeSslPolicy( sslServerResource, TESTING ) );

        server.start();
        client = new SecureClient( makeSslPolicy( sslClientResource, TESTING ) );
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
    void partiesWithMutualTrustThroughCAShouldCommunicate() throws Exception
    {
        // given
        SslResource sslServerResource = caSignedKeyId( 0 ).trustSignedByCA().install( testDir.directory( "server" ) );
        SslResource sslClientResource = caSignedKeyId( 1 ).trustSignedByCA().install( testDir.directory( "client" ) );

        server = new SecureServer( makeSslPolicy( sslServerResource, TESTING ) );

        server.start();
        client = new SecureClient( makeSslPolicy( sslClientResource, TESTING ) );
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
    void serverShouldNotCommunicateWithUntrustedClient() throws Exception
    {
        // given
        SslResource sslClientResource = selfSignedKeyId( 1 ).trustKeyId( 0 ).install( testDir.directory( "client" ) );
        SslResource sslServerResource = selfSignedKeyId( 0 ).trustKeyId( UNRELATED_ID ).install( testDir.directory( "server" ) );

        server = new SecureServer( makeSslPolicy( sslServerResource, TESTING ) );

        server.start();
        client = new SecureClient( makeSslPolicy( sslClientResource, TESTING ) );
        client.connect( server.port() );

        var e = assertThrows( ExecutionException.class, () -> client.sslHandshakeFuture().get( 1, MINUTES ) );
        assertThat( e ).hasCauseInstanceOf( SSLException.class );
    }

    @Test
    void clientShouldNotCommunicateWithUntrustedServer() throws Exception
    {
        // given
        SslResource sslClientResource = selfSignedKeyId( 0 ).trustKeyId( UNRELATED_ID ).install( testDir.directory( "client" ) );
        SslResource sslServerResource = selfSignedKeyId( 1 ).trustKeyId( 0 ).install( testDir.directory( "server" ) );

        server = new SecureServer( makeSslPolicy( sslServerResource, TESTING ) );

        server.start();
        client = new SecureClient( makeSslPolicy( sslClientResource, TESTING ) );
        client.connect( server.port() );

        var e = assertThrows( ExecutionException.class, () -> client.sslHandshakeFuture().get( 1, MINUTES ) );
        assertThat( e ).hasCauseInstanceOf( SSLException.class );
    }

    @Test
    void partiesWithMutualTrustThroughCAShouldNotCommunicateWhenServerRevoked() throws Exception
    {
        // given
        SslResource sslServerResource = caSignedKeyId( 0 ).trustSignedByCA().install( testDir.directory( "server" ) );
        SslResource sslClientResource = caSignedKeyId( 1 ).trustSignedByCA().revoke( 0 ).install( testDir.directory( "client" ) );

        server = new SecureServer( makeSslPolicy( sslServerResource, TESTING ) );

        server.start();
        client = new SecureClient( makeSslPolicy( sslClientResource, TESTING ) );
        client.connect( server.port() );

        var e = assertThrows( ExecutionException.class, () -> client.sslHandshakeFuture().get( 1, MINUTES ) );
        assertThat( e ).hasCauseInstanceOf( SSLException.class );
    }

    @Test
    void partiesWithMutualTrustThroughCAShouldNotCommunicateWhenClientRevoked() throws Exception
    {
        // given
        SslResource sslServerResource = caSignedKeyId( 0 ).trustSignedByCA().revoke( 1 ).install( testDir.directory( "server" ) );
        SslResource sslClientResource = caSignedKeyId( 1 ).trustSignedByCA().install( testDir.directory( "client" ) );

        server = new SecureServer( makeSslPolicy( sslServerResource, TESTING ) );

        server.start();
        client = new SecureClient( makeSslPolicy( sslClientResource, TESTING ) );
        client.connect( server.port() );

        var e = assertThrows( ExecutionException.class, () -> client.sslHandshakeFuture().get( 1, MINUTES ) );
        assertThat( e ).hasCauseInstanceOf( SSLException.class );
    }
}
