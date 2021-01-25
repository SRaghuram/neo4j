/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.ssl;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.handler.ssl.SslProvider;
import io.netty.util.ReferenceCountUtil;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledOnOs;

import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static com.neo4j.ssl.SecureServer.RESPONSE;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.junit.jupiter.api.condition.OS.LINUX;
import static org.junit.jupiter.api.condition.OS.MAC;
import static org.junit.jupiter.api.condition.OS.WINDOWS;
import static org.neo4j.configuration.ssl.SslPolicyScope.TESTING;
import static org.neo4j.function.ThrowingAction.executeAll;
import static org.neo4j.ssl.SslResourceBuilder.selfSignedKeyId;

/**
 * This test depends on the statically linked uber-jar with boring ssl: http://netty.io/wiki/forked-tomcat-native.html.
 * The uber-jar comes from io.netty:netty-tcnative-boringssl-static maven dependency;
 */
@TestDirectoryExtension
class SslPlatformTest
{
    private static final byte[] REQUEST = {1, 2, 3, 4};

    @Inject
    private TestDirectory testDirectory;

    private SecureServer server;
    private SecureClient client;
    private ByteBuf expected;

    @AfterEach
    void afterEach() throws Exception
    {
        executeAll(
                this::stopClient,
                this::stopServer,
                () -> ReferenceCountUtil.release( expected ) );
    }

    @Test
    @EnabledOnOs( {LINUX, MAC, WINDOWS} )
    void shouldSupportOpenSSLOnSupportedPlatforms() throws Exception
    {
        // given
        var sslServerResource = selfSignedKeyId( 0 ).trustKeyId( 1 ).install( testDirectory.directory( "server" ) );
        var sslClientResource = selfSignedKeyId( 1 ).trustKeyId( 0 ).install( testDirectory.directory( "client" ) );

        server = new SecureServer( SslContextFactory.makeSslPolicy( sslServerResource, SslProvider.OPENSSL, TESTING ) );

        server.start();
        client = new SecureClient( SslContextFactory.makeSslPolicy( sslClientResource, SslProvider.OPENSSL, TESTING ) );
        client.connect( server.port() );

        // when
        var request = Unpooled.wrappedBuffer( REQUEST );
        client.channel().writeAndFlush( request );

        // then
        expected = Unpooled.wrappedBuffer( RESPONSE );
        client.sslHandshakeFuture().get( 1, MINUTES );
        client.assertResponse( expected );
    }

    private void stopClient()
    {
        if ( client != null )
        {
            client.disconnect();
        }
    }

    private void stopServer()
    {
        if ( server != null )
        {
            server.stop();
        }
    }
}
