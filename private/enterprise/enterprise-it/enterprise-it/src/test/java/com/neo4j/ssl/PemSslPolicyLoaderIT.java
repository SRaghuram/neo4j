/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.ssl;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import org.hamcrest.Matchers;
import org.hamcrest.core.IsIterableContaining;
import org.junit.Rule;
import org.junit.Test;

import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.neo4j.configuration.Config;
import org.neo4j.logging.FormattedLogProvider;
import org.neo4j.logging.Level;
import org.neo4j.logging.LogProvider;
import org.neo4j.ssl.SslPolicy;
import org.neo4j.ssl.config.SslPolicyLoader;
import org.neo4j.test.rule.TestDirectory;

import static com.neo4j.ssl.HostnameVerificationHelper.aConfig;
import static com.neo4j.ssl.HostnameVerificationHelper.trust;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.neo4j.configuration.ssl.SslPolicyScope.TESTING;

public class PemSslPolicyLoaderIT
{
    @Rule
    public TestDirectory testDirectory = TestDirectory.testDirectory();

    private static final LogProvider LOG_PROVIDER = FormattedLogProvider.withDefaultLogLevel( Level.DEBUG ).toOutputStream( System.out );

    @Test
    public void certificatesWithInvalidCommonNameAreRejected() throws Exception
    {
        // given server has a certificate that matches an invalid hostname
        Config serverConfig = aConfig( "invalid-not-localhost", testDirectory, TESTING );

        // and client has any certificate (valid), since hostname validation is done from the client side
        Config clientConfig = aConfig( "localhost", testDirectory, TESTING );

        trust( serverConfig, clientConfig, TESTING );
        trust( clientConfig, serverConfig, TESTING );

        // and setup
        SslPolicy serverPolicy = SslPolicyLoader.create( serverConfig, LOG_PROVIDER ).getPolicy( TESTING );
        SslPolicy clientPolicy = SslPolicyLoader.create( clientConfig, LOG_PROVIDER ).getPolicy( TESTING );
        SecureServer secureServer = new SecureServer( serverPolicy );
        secureServer.start();
        SecureClient secureClient = new SecureClient( clientPolicy );

        // when client connects to server with a non-matching hostname
        clientCannotCommunicateWithServer( secureClient, secureServer, "No subject alternative DNS name matching localhost found." );
    }

    @Test
    public void normalBehaviourIfServerCertificateMatchesClientExpectation() throws Exception
    {
        // given server has valid hostname
        Config serverConfig = aConfig( "localhost", testDirectory, TESTING );

        // and client has invalid hostname (which is irrelevant for hostname verification)
        Config clientConfig = aConfig( "invalid-localhost", testDirectory, TESTING );

        trust( serverConfig, clientConfig, TESTING );
        trust( clientConfig, serverConfig, TESTING );

        // and setup
        SslPolicy serverPolicy = SslPolicyLoader.create( serverConfig, LOG_PROVIDER ).getPolicy( TESTING );
        SslPolicy clientPolicy = SslPolicyLoader.create( clientConfig, LOG_PROVIDER ).getPolicy( TESTING );
        SecureServer secureServer = new SecureServer( serverPolicy );
        secureServer.start();
        SecureClient secureClient = new SecureClient( clientPolicy );

        // then
        clientCanCommunicateWithServer( secureClient, secureServer );
    }

    static void clientCanCommunicateWithServer( SecureClient secureClient, SecureServer secureServer )
            throws InterruptedException, TimeoutException, ExecutionException
    {
        int port = secureServer.port();
        try
        {
            secureClient.connect( port );
            ByteBuf request = ByteBufAllocator.DEFAULT.buffer().writeBytes( new byte[]{1, 2, 3, 4} );
            secureClient.channel().writeAndFlush( request );

            ByteBuf expected = ByteBufAllocator.DEFAULT.buffer().writeBytes( SecureServer.RESPONSE );
            assertTrue( secureClient.sslHandshakeFuture().get( 1, MINUTES ).isActive() );
            secureClient.assertResponse( expected );
            expected.release();
        }
        finally
        {
            secureClient.disconnect();
            secureServer.stop();
        }
    }

    static void clientCannotCommunicateWithServer( SecureClient secureClient, SecureServer secureServer, String expectedMessage ) throws InterruptedException
    {
        int port = secureServer.port();
        try
        {
            secureClient.connect( port );

            // then handshake complete with exception describing hostname mismatch
            secureClient.sslHandshakeFuture().get( 1, MINUTES );
        }
        catch ( ExecutionException e )
        {
            assertThat( causes( e ).map( Throwable::getMessage ).collect( Collectors.toList() ),
                    IsIterableContaining.hasItem( Matchers.containsString( expectedMessage ) ) );
        }
        catch ( TimeoutException e )
        {
            throw new RuntimeException( e );
        }
        finally
        {
            secureClient.disconnect();
            secureServer.stop();
        }
    }

    private static Stream<Throwable> causes( Throwable throwable )
    {
        Stream<Throwable> thisStream = Stream.of( throwable ).filter( Objects::nonNull );
        if ( throwable != null && throwable.getCause() != null )
        {
            return Stream.concat( thisStream, causes( throwable.getCause() ) );
        }
        else
        {
            return thisStream;
        }
    }
}
