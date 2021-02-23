/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.fabric.driver;

import com.neo4j.configuration.FabricEnterpriseConfig;
import com.neo4j.fabric.auth.CredentialsProvider;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslHandler;
import org.bouncycastle.operator.OperatorCreationException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.security.cert.CertificateException;
import java.time.Clock;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLHandshakeException;

import org.neo4j.bolt.runtime.AccessMode;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.ssl.ClientAuth;
import org.neo4j.configuration.ssl.SslPolicyConfig;
import org.neo4j.driver.exceptions.ServiceUnavailableException;
import org.neo4j.fabric.executor.ExecutionOptions;
import org.neo4j.fabric.executor.Location;
import org.neo4j.fabric.transaction.FabricTransactionInfo;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.ssl.SslPolicy;
import org.neo4j.ssl.SslResource;
import org.neo4j.ssl.config.SslPolicyLoader;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.test.ssl.SelfSignedCertificateFactory;
import org.neo4j.values.virtual.MapValue;

import static com.neo4j.fabric.TestUtils.createUri;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.configuration.ssl.SslPolicyScope.TESTING;
import static org.neo4j.ssl.SslResourceBuilder.caSignedKeyId;
import static org.neo4j.ssl.SslResourceBuilder.selfSignedKeyId;

@TestDirectoryExtension
abstract class AbstractDriverSslTest
{
//    {
//        // for debugging
//        System.setProperty( "javax.net.debug", "all" );
//    }

    @Inject
    private TestDirectory testDirectory;

    private Server server;
    private DriverPool driverPool;

    @AfterEach
    void afterEach()
    {
        if ( server != null )
        {
            server.stop();
        }

        if ( driverPool != null )
        {
            driverPool.stop();
        }
    }

    @Test
    void testServerCertificateTrustedDirectly() throws Exception
    {
        SslResource sslServerResource = selfSignedKeyId( 0 ).trustKeyId( 1 ).install( testDirectory.directory( "server" ) );
        SslResource sslClientResource = selfSignedKeyId( 1 ).trustKeyId( 0 ).install( testDirectory.directory( "client" ) );

        server = startServer( sslServerResource, ClientAuth.REQUIRE );

        testHandshakeSuccess( sslClientResource );
        assertTrue( server.connectionEstablished.get() );
    }

    @Test
    void testServerCertificateTrustedThroughCA() throws Exception
    {
        SslResource sslServerResource = caSignedKeyId( 0 ).trustSignedByCA().install( testDirectory.directory( "server" ) );
        SslResource sslClientResource = caSignedKeyId( 1 ).trustSignedByCA().install( testDirectory.directory( "client" ) );

        server = startServer( sslServerResource, ClientAuth.REQUIRE  );

        testHandshakeSuccess( sslClientResource  );
        assertTrue( server.connectionEstablished.get() );
    }

    @Test
    void testServerCertificateNotTrusted() throws Exception
    {
        SslResource sslClientResource = selfSignedKeyId( 0 ).trustKeyId( 5 ).install( testDirectory.directory( "client" ) );
        SslResource sslServerResource = selfSignedKeyId( 1 ).trustKeyId( 0 ).install( testDirectory.directory( "server" ) );

        server = startServer( sslServerResource, ClientAuth.REQUIRE );

        testSslHandshakeFail( sslClientResource, "unable to find valid certification path to requested target" );
        assertFalse( server.connectionEstablished.get() );
    }

    @Test
    void testServerCertificateRevoked() throws Exception
    {
        SslResource sslServerResource = caSignedKeyId( 0 ).trustSignedByCA().install( testDirectory.directory( "server" ) );
        SslResource sslClientResource = caSignedKeyId( 1 ).trustSignedByCA().revoke( 0 ).install( testDirectory.directory( "client" ) );

        server = startServer( sslServerResource, ClientAuth.REQUIRE );

        testSslHandshakeFail( sslClientResource, "PKIX path validation failed" );
        assertFalse( server.connectionEstablished.get() );
    }

    @Test
    void testHostnameVerificationSuccess() throws IOException, GeneralSecurityException, OperatorCreationException
    {
        SslDir sslServerResource = selfSignedCertificate( testDirectory.absolutePath().resolve( "server" ) );
        SslDir sslClientResource = trust( sslServerResource, testDirectory.absolutePath().resolve( "client" ) );

        server = startServer( sslServerResource, ClientAuth.NONE );

        testHostnameVerification( sslClientResource, "localhost", ServiceUnavailableException.class, "Connection to the database terminated" );
        assertTrue( server.connectionEstablished.get() );
    }

    @Test
    void testHostnameVerificationFailure() throws IOException, GeneralSecurityException, OperatorCreationException
    {
        SslDir sslServerResource = selfSignedCertificate( testDirectory.absolutePath().resolve( "server" ) );
        SslDir sslClientResource = trust( sslServerResource, testDirectory.absolutePath().resolve( "client" ) );

        server = startServer( sslServerResource, ClientAuth.NONE );

        testHostnameVerification( sslClientResource, "127.0.0.1", CertificateException.class,
                "No subject alternative names matching IP address 127.0.0.1 found" );
        assertFalse( server.connectionEstablished.get() );
    }

    abstract Map<String,String> getSettingValues( String hostname, SslDir sslResource, String policyBaseDir, boolean verifyHostname );

    abstract DriverConfigFactory getConfigFactory( FabricEnterpriseConfig fabricConfig, Config serverConfig, SslPolicyLoader sslPolicyLoader );

    abstract boolean requiresPrivateKey();

    private <T extends Exception> void testHostnameVerification( SslDir sslResource, String host, Class<T> errorCause, String errorMessage )
    {
        testConnection( sslResource, host, errorCause, errorMessage, true );
    }

    private void testHandshakeSuccess( SslResource sslResource )
    {
        testConnection( toDir( sslResource ), "localhost", ServiceUnavailableException.class, "Connection to the database terminated", false );
    }

    private void testSslHandshakeFail( SslResource sslResource, String expectedErrorMessage )
    {
        testConnection( toDir( sslResource ), "localhost", SSLHandshakeException.class, expectedErrorMessage, false );
    }

    private <T extends Exception> void testConnection( SslDir sslResource, String host, Class<T> errorCause, String errorMessage, boolean verifyHostname )
    {
        var policyBaseDir = testDirectory.directory( "client" ).toAbsolutePath().toString();
        var uri = "bolt://" + host + ":" + server.port();
        var properties = getSettingValues( uri, sslResource, policyBaseDir, verifyHostname );
        var config = Config.newBuilder()
                           .setRaw( properties )
                           .build();

        var fabricConfig = FabricEnterpriseConfig.from( config );
        var sslLoader = SslPolicyLoader.create( config, NullLogProvider.nullLogProvider() );

        var jobScheduler = mock( JobScheduler.class );
        var credentialsProvider = mock( CredentialsProvider.class );

        var driverConfigFactory = getConfigFactory( fabricConfig, config, sslLoader );
        driverPool = new DriverPool( jobScheduler, driverConfigFactory, fabricConfig, Clock.systemUTC(), credentialsProvider );
        driverPool.start();
        var driver = driverPool.getDriver( new Location.Remote.External( 0, null, createUri( uri ), null ), null );

        var transactionInfo = mock( FabricTransactionInfo.class );
        when( transactionInfo.getTxTimeout() ).thenReturn( Duration.ZERO );
        try
        {
            var location = new Location.Remote.External( 0, null, null, null );
            driver.run( "RETURN 1", MapValue.EMPTY, location, new ExecutionOptions(), AccessMode.WRITE, transactionInfo, List.of() )
                    .columns()
                    .collectList()
                    .block();
        }
        catch ( Exception e )
        {

            // the interaction with the server will always end with error, because the server does not understand Bolt.
            // so it is important to find out if SSL handshake succeeded
            Throwable cause = e;
            while ( cause.getCause() != null && !errorCause.isInstance( cause ) )
            {
                cause = cause.getCause();
            }

            assertEquals( errorCause, cause.getClass() );
            assertThat( cause.getMessage() ).contains( errorMessage );
        }
    }

    private SslDir selfSignedCertificate( Path directory ) throws GeneralSecurityException, IOException, OperatorCreationException
    {
        SelfSignedCertificateFactory certFactory = new SelfSignedCertificateFactory();

        var certFile = directory.resolve( "public.crt" );
        var keyFile = directory.resolve( "private.key" );
        var trustedDir = directory.resolve( "trusted" );
        certFactory.createSelfSignedCertificate( certFile, keyFile, "localhost" );

        return new SslDir( keyFile, certFile, trustedDir, directory.resolve( "revoked" ) );
    }

    private SslDir trust( SslDir sslDir, Path directory ) throws IOException, GeneralSecurityException, OperatorCreationException
    {
        var trustedDir = directory.resolve( "trusted" );
        Files.createDirectories(trustedDir);
        Files.copy(sslDir.publicCertificate, trustedDir.resolve( "server.crt" ));

        // some policies just need a private key
        // (they just refuse to load without one)
        // even though it is unimportant for the given test
        if ( requiresPrivateKey() )
        {
            SelfSignedCertificateFactory certFactory = new SelfSignedCertificateFactory();

            var certFile = directory.resolve( "public.crt" );
            var keyFile = directory.resolve( "private.key" );
            certFactory.createSelfSignedCertificate( certFile, keyFile, "localhost" );
        }

        return new SslDir( directory.resolve( "private.key" ), directory.resolve( "public.crt" ), trustedDir, directory.resolve( "revoked" ) );
    }

    private SslDir toDir( SslResource sslResource )
    {
        return new SslDir(
                sslResource.privateKey(),
                sslResource.publicCertificate(),
                sslResource.trustedDirectory(),
                sslResource.revokedDirectory()
        );
    }

    private Server startServer( SslResource sslResource, ClientAuth clientAuth ) throws IOException
    {
        var sslDir = new SslDir(
                sslResource.privateKey(),
                sslResource.publicCertificate(),
                sslResource.trustedDirectory(),
                sslResource.revokedDirectory()
        );

        return startServer( sslDir, clientAuth );
    }

    private Server startServer( SslDir sslDir, ClientAuth clientAuth ) throws IOException
    {
        var server = new Server( sslDir, clientAuth );
        server.start();
        return server;
    }

    protected static class Server
    {

        private final AtomicBoolean connectionEstablished = new AtomicBoolean( false );
        private SslContext sslContext;
        private Channel channel;
        private NioEventLoopGroup eventLoopGroup;

        Server( SslDir sslDir, ClientAuth clientAuth ) throws IOException
        {
            SslPolicy sslPolicy = makeSslPolicy( sslDir, clientAuth );
            this.sslContext = sslPolicy.nettyServerContext();
        }

        void start()
        {
            eventLoopGroup = new NioEventLoopGroup();
            ServerBootstrap bootstrap = new ServerBootstrap()
                    .group( eventLoopGroup )
                    .channel( NioServerSocketChannel.class )
                    .option( ChannelOption.SO_REUSEADDR, true )
                    .localAddress( 0 )
                    .childHandler( new ChannelInitializer<SocketChannel>()
                    {
                        @Override
                        protected void initChannel( SocketChannel ch )
                        {
                            ChannelPipeline pipeline = ch.pipeline();
                            SSLEngine sslEngine = sslContext.newEngine( ch.alloc() );
                            SslHandler sslHandler = new SslHandler( sslEngine );
                            pipeline.addLast( sslHandler );

                            pipeline.addLast( new Handler( connectionEstablished ) );
                        }
                    } );

            channel = bootstrap.bind().syncUninterruptibly().channel();
        }

        void stop()
        {
            channel.close().awaitUninterruptibly();
            channel = null;
            eventLoopGroup.shutdownGracefully( 0, 0, SECONDS );
        }

        int port()
        {
            return ((InetSocketAddress) channel.localAddress()).getPort();
        }

        private SslPolicy makeSslPolicy( SslDir sslDir, ClientAuth clientAuth )
        {
            Config.Builder config = Config.newBuilder();

            var policyConfig = SslPolicyConfig.forScope( TESTING );
            var baseDirectory = sslDir.privateKey.getParent();

            config.set( policyConfig.enabled, Boolean.TRUE );
            config.set( policyConfig.base_directory, baseDirectory.toAbsolutePath() );
            config.set( policyConfig.private_key, sslDir.privateKey.toAbsolutePath() );
            config.set( policyConfig.public_certificate, sslDir.publicCertificate.toAbsolutePath() );
            config.set( policyConfig.trusted_dir, sslDir.trustedDirectory.toAbsolutePath() );
            config.set( policyConfig.revoked_dir, sslDir.revokedDirectory.toAbsolutePath() );
            config.set( policyConfig.verify_hostname, false );
            config.set( policyConfig.client_auth, clientAuth );

            SslPolicyLoader sslPolicyFactory = SslPolicyLoader.create( config.build(), NullLogProvider.getInstance() );

            return sslPolicyFactory.getPolicy( TESTING );
        }

        static class Handler extends SimpleChannelInboundHandler<ByteBuf>
        {
            private final AtomicBoolean connectSuccess;

            Handler( AtomicBoolean connectSuccess )
            {
                this.connectSuccess = connectSuccess;
            }

            @Override
            protected void channelRead0( ChannelHandlerContext ctx, ByteBuf msg )
            {
                connectSuccess.set( true );
                ctx.channel().close();
            }

            @Override
            public void exceptionCaught( ChannelHandlerContext ctx, Throwable cause )
            {
               // cause.printStackTrace(); // for debugging
            }
        }
    }

    protected static final class SslDir
    {
        private final Path privateKey;
        private final Path publicCertificate;
        private final Path trustedDirectory;
        private final Path revokedDirectory;

        SslDir( Path privateKey, Path publicCertificate, Path trustedDirectory, Path revokedDirectory )
        {
            this.privateKey = privateKey;
            this.publicCertificate = publicCertificate;
            this.trustedDirectory = trustedDirectory;
            this.revokedDirectory = revokedDirectory;
        }

        protected Path getPrivateKey()
        {
            return privateKey;
        }

        protected Path getPublicCertificate()
        {
            return publicCertificate;
        }

        protected Path getTrustedDirectory()
        {
            return trustedDirectory;
        }

        protected Path getRevokedDirectory()
        {
            return revokedDirectory;
        }
    }
}
