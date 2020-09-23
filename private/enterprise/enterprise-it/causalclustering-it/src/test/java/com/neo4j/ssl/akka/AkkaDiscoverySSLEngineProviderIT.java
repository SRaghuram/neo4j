/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.ssl.akka;

import akka.actor.AbstractLoggingActor;
import akka.actor.ActorRef;
import akka.actor.ActorSelection;
import akka.actor.ActorSystem;
import akka.actor.Address;
import akka.actor.BootstrapSetup;
import akka.actor.Props;
import akka.actor.ProviderSelection;
import akka.actor.setup.ActorSystemSetup;
import akka.event.Logging;
import akka.japi.pf.ReceiveBuilder;
import akka.remote.artery.tcp.SSLEngineProvider;
import akka.remote.artery.tcp.SSLEngineProviderSetup;
import akka.testkit.TestProbe;
import com.neo4j.causalclustering.discovery.AkkaDiscoverySSLEngineProvider;
import com.typesafe.config.ConfigFactory;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import scala.concurrent.duration.FiniteDuration;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.stream.Stream;
import javax.net.ssl.SSLEngine;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.ssl.ClientAuth;
import org.neo4j.configuration.ssl.SslPolicyConfig;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.ssl.SslPolicy;
import org.neo4j.ssl.SslResource;
import org.neo4j.ssl.config.SslPolicyLoader;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.ports.PortAuthority;
import org.neo4j.test.rule.TestDirectory;

import static com.neo4j.ssl.HostnameVerificationHelper.aConfig;
import static com.neo4j.ssl.HostnameVerificationHelper.trust;
import static com.neo4j.ssl.SslContextFactory.SslParameters.protocols;
import static com.neo4j.ssl.SslContextFactory.makeSslPolicy;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.containsString;
import static org.neo4j.configuration.ssl.SslPolicyScope.CLUSTER;
import static org.neo4j.ssl.SslResourceBuilder.caSignedKeyId;
import static org.neo4j.ssl.SslResourceBuilder.selfSignedKeyId;

@TestDirectoryExtension
class AkkaDiscoverySSLEngineProviderIT
{
    private static final String MSG = "When in doubt, burn it to the ground and start from scratch";

    private static final String TLSv11 = "TLSv1.1";
    private static final String TLSv12 = "TLSv1.2";
    private static final String NEW_CIPHER_A = "TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA";
    private static final String NEW_CIPHER_B = "TLS_RSA_WITH_AES_128_CBC_SHA256";

    private static final int UNRELATED_ID = 5; // SslContextFactory requires us to trust something

    private static final FiniteDuration TIMEOUT = new FiniteDuration( 30L, TimeUnit.SECONDS );

    @Inject
    private TestDirectory testDir;

    @ParameterizedTest
    @EnumSource( SslEngineProviderFactory.class )
    void shouldConnectWithMutualTrust( SslEngineProviderFactory sslEngineProviderFactory ) throws Throwable
    {
        SslResource sslServerResource = selfSignedKeyId( 0 ).trustKeyId( 1 ).install( testDir.directory( "server" ) );
        SslResource sslClientResource = selfSignedKeyId( 1 ).trustKeyId( 0 ).install( testDir.directory( "client" ) );

        testConnection( sslEngineProviderFactory, sslClientResource, sslServerResource, this::accept );
    }

    @ParameterizedTest
    @EnumSource( SslEngineProviderFactory.class )
    void shouldConnectWithMutualTrustViaCA( SslEngineProviderFactory sslEngineProviderFactory ) throws Throwable
    {
        SslResource sslServerResource = caSignedKeyId( 0 ).trustSignedByCA().install( testDir.directory( "server" ) );
        SslResource sslClientResource = caSignedKeyId( 1 ).trustSignedByCA().install( testDir.directory( "client" ) );

        testConnection( sslEngineProviderFactory, sslClientResource, sslServerResource, this::accept );
    }

    @ParameterizedTest
    @EnumSource( SslEngineProviderFactory.class )
    void shouldNotConnectWithUntrustedClient( SslEngineProviderFactory sslEngineProviderFactory ) throws Throwable
    {
        SslResource sslClientResource = selfSignedKeyId( 1 ).trustKeyId( 0 ).install( testDir.directory( "client" ) );
        SslResource sslServerResource = selfSignedKeyId( 0 ).trustKeyId( UNRELATED_ID ).install( testDir.directory( "server" ) );

        testConnection( sslEngineProviderFactory, sslClientResource, sslServerResource, this::decline );
    }

    @ParameterizedTest
    @EnumSource( SslEngineProviderFactory.class )
    void shouldNotConnectWithUntrustedServer( SslEngineProviderFactory sslEngineProviderFactory ) throws Throwable
    {
        SslResource sslClientResource = selfSignedKeyId( 0 ).trustKeyId( UNRELATED_ID ).install( testDir.directory( "client" ) );
        SslResource sslServerResource = selfSignedKeyId( 1 ).trustKeyId( 0 ).install( testDir.directory( "server" ) );

        testConnection( sslEngineProviderFactory, sslClientResource, sslServerResource, this::decline );
    }

    @ParameterizedTest
    @EnumSource( SslEngineProviderFactory.class )
    void shouldNotConnectWhenTrustedByCAAndServerRevoked( SslEngineProviderFactory sslEngineProviderFactory ) throws Throwable
    {
        SslResource sslServerResource = caSignedKeyId( 0 ).trustSignedByCA().install( testDir.directory( "server" ) );
        SslResource sslClientResource = caSignedKeyId( 1 ).trustSignedByCA().revoke( 0 ).install( testDir.directory( "client" ) );

        testConnection( sslEngineProviderFactory, sslClientResource, sslServerResource, this::decline );
    }

    @ParameterizedTest
    @EnumSource( SslEngineProviderFactory.class )
    void shouldNotConnectWhenTrustedByCAAndClientRevoked( SslEngineProviderFactory sslEngineProviderFactory ) throws Throwable
    {
        SslResource sslServerResource = caSignedKeyId( 0 ).trustSignedByCA().revoke( 1 ).install( testDir.directory( "server" ) );
        SslResource sslClientResource = caSignedKeyId( 1 ).trustSignedByCA().install( testDir.directory( "client" ) );

        testConnection( sslEngineProviderFactory, sslClientResource, sslServerResource, this::decline );
    }

    @ParameterizedTest
    @EnumSource( SslEngineProviderFactory.class )
    void shouldConnectIfProtocolsInCommon( SslEngineProviderFactory sslEngineProviderFactory ) throws Throwable
    {
        SslResource sslServerResource = selfSignedKeyId( 0 ).trustKeyId( 1 ).install( testDir.directory( "server" ) );
        SslResource sslClientResource = selfSignedKeyId( 1 ).trustKeyId( 0 ).install( testDir.directory( "client" ) );
        SslPolicy serverSslPolicy = makeSslPolicy( sslServerResource, protocols( TLSv12 ).ciphers(), CLUSTER );
        SslPolicy clientSslPolicy = makeSslPolicy( sslClientResource, protocols( TLSv12 ).ciphers(), CLUSTER );

        testConnection( sslEngineProviderFactory, clientSslPolicy, serverSslPolicy, this::accept );
    }

    @ParameterizedTest
    @EnumSource( SslEngineProviderFactory.class )
    void shouldNotConnectIfNoProtocolsInCommon( SslEngineProviderFactory sslEngineProviderFactory ) throws Throwable
    {
        SslResource sslServerResource = selfSignedKeyId( 0 ).trustKeyId( 1 ).install( testDir.directory( "server" ) );
        SslResource sslClientResource = selfSignedKeyId( 1 ).trustKeyId( 0 ).install( testDir.directory( "client" ) );
        SslPolicy serverSslPolicy = makeSslPolicy( sslServerResource, protocols( TLSv12 ).ciphers(), CLUSTER );
        SslPolicy clientSslPolicy = makeSslPolicy( sslClientResource, protocols( TLSv11 ).ciphers(), CLUSTER );

        testConnection( sslEngineProviderFactory, clientSslPolicy, serverSslPolicy, this::decline );
    }

    @ParameterizedTest
    @EnumSource( SslEngineProviderFactory.class )
    void shouldConnectIfCiphersInCommon( SslEngineProviderFactory sslEngineProviderFactory ) throws Throwable
    {
        SslResource sslServerResource = selfSignedKeyId( 0 ).trustKeyId( 1 ).install( testDir.directory( "server" ) );
        SslResource sslClientResource = selfSignedKeyId( 1 ).trustKeyId( 0 ).install( testDir.directory( "client" ) );
        SslPolicy serverSslPolicy = makeSslPolicy( sslServerResource, protocols().ciphers( NEW_CIPHER_A ), CLUSTER );
        SslPolicy clientSslPolicy = makeSslPolicy( sslClientResource, protocols().ciphers( NEW_CIPHER_A ), CLUSTER );

        testConnection( sslEngineProviderFactory, clientSslPolicy, serverSslPolicy, this::accept );
    }

    @ParameterizedTest
    @EnumSource( SslEngineProviderFactory.class )
    void shouldNotConnectIfNoCiphersInCommon( SslEngineProviderFactory sslEngineProviderFactory ) throws Throwable
    {
        SslResource sslServerResource = selfSignedKeyId( 0 ).trustKeyId( 1 ).install( testDir.directory( "server" ) );
        SslResource sslClientResource = selfSignedKeyId( 1 ).trustKeyId( 0 ).install( testDir.directory( "client" ) );
        SslPolicy serverSslPolicy = makeSslPolicy( sslServerResource, protocols().ciphers( NEW_CIPHER_A ), CLUSTER );
        SslPolicy clientSslPolicy = makeSslPolicy( sslClientResource, protocols().ciphers( NEW_CIPHER_B ), CLUSTER );

        testConnection( sslEngineProviderFactory, clientSslPolicy, serverSslPolicy, this::decline );
    }

    @ParameterizedTest
    @EnumSource( SslEngineProviderFactory.class )
    void shouldNotConnectIfInvalidCommonNameOnServer( SslEngineProviderFactory sslEngineProviderFactory ) throws Throwable
    {
        Config serverConfig = aConfig( "invalid", testDir, CLUSTER );

        Config clientConfig = aConfig( "localhost", testDir, CLUSTER );

        trust( serverConfig, clientConfig, CLUSTER );
        trust( clientConfig, serverConfig, CLUSTER );

        SslPolicy serverPolicy = SslPolicyLoader.create( serverConfig, NullLogProvider.getInstance() ).getPolicy( CLUSTER );
        SslPolicy clientPolicy = SslPolicyLoader.create( clientConfig, NullLogProvider.getInstance() ).getPolicy( CLUSTER );

        testConnection( sslEngineProviderFactory, clientPolicy, serverPolicy, this::decline );
    }

    @ParameterizedTest
    @EnumSource( SslEngineProviderFactory.class )
    void shouldConnectIfValidCommonName( SslEngineProviderFactory sslEngineProviderFactory ) throws Throwable
    {
        Config serverConfig = aConfig( "localhost", testDir, CLUSTER );

        Config clientConfig = aConfig( "localhost", testDir, CLUSTER );

        trust( serverConfig, clientConfig, CLUSTER );
        trust( clientConfig, serverConfig, CLUSTER );

        SslPolicy serverPolicy = SslPolicyLoader.create( serverConfig, NullLogProvider.getInstance() ).getPolicy( CLUSTER );
        SslPolicy clientPolicy = SslPolicyLoader.create( clientConfig, NullLogProvider.getInstance() ).getPolicy( CLUSTER );

        testConnection( sslEngineProviderFactory, clientPolicy, serverPolicy, this::accept );
    }

    @ParameterizedTest
    @EnumSource( SslEngineProviderFactory.class )
    void shouldConnectWithHostnameVerificationAndClientAuth( SslEngineProviderFactory sslEngineProviderFactory ) throws Throwable
    {
        SslPolicyConfig policy = SslPolicyConfig.forScope( CLUSTER );

        Config serverConfig = aConfig( "localhost", testDir, CLUSTER );
        serverConfig.set( policy.client_auth, ClientAuth.REQUIRE );

        Config clientConfig = aConfig( "localhost", testDir, CLUSTER );
        clientConfig.set( policy.client_auth, ClientAuth.REQUIRE );

        trust( serverConfig, clientConfig, CLUSTER );
        trust( clientConfig, serverConfig, CLUSTER );

        SslPolicy serverPolicy = SslPolicyLoader.create( serverConfig, NullLogProvider.getInstance() ).getPolicy( CLUSTER );
        SslPolicy clientPolicy = SslPolicyLoader.create( clientConfig, NullLogProvider.getInstance() ).getPolicy( CLUSTER );

        testConnection( sslEngineProviderFactory, clientPolicy, serverPolicy, this::accept );
    }

    private ActorSystem createActorSystem( String name, SSLEngineProvider sslEngineProvider )
    {
        BootstrapSetup bootstrap = BootstrapSetup.create().withActorRefProvider( ProviderSelection.remote() ).withConfig( config() );
        ActorSystemSetup actorSystemSetup = ActorSystemSetup.create( bootstrap ).withSetup( SSLEngineProviderSetup.create( system -> sslEngineProvider ) );

        return ActorSystem.create( "ssl-test-" + name, actorSystemSetup );
    }

    private com.typesafe.config.Config config()
    {
        int port = PortAuthority.allocatePort();
        Map<String,Object> configMap = new HashMap<>();

        configMap.put( "akka.remote.artery.enabled", true );
        configMap.put( "akka.remote.artery.transport", "tls-tcp" );

        configMap.put( "akka.remote.artery.canonical.hostname", "localhost" );
        configMap.put( "akka.remote.artery.canonical.port", port );

        configMap.put( "akka.remote.artery.bind.hostname", "localhost" );
        configMap.put( "akka.remote.artery.bind.port", port );

        configMap.put( "akka.loglevel", "debug" );

        // Comment this out to log to stdout
        configMap.put( "akka.loggers", Collections.singletonList( NullLoggingActor.class.getCanonicalName() ) );

        return ConfigFactory.empty()
                .withFallback( ConfigFactory.parseMap( configMap ) )
                .withFallback( ConfigFactory.defaultReference() );
    }

    private void testConnection( SslEngineProviderFactory sslEngineProviderFactory, SslResource clientSslResource, SslResource serverSslResource,
            BiConsumer<TestProbe,TestProbe> verify ) throws Exception
    {
        testConnection( sslEngineProviderFactory, makeSslPolicy( clientSslResource, CLUSTER ), makeSslPolicy( serverSslResource, CLUSTER ), verify );
    }

    private void testConnection( SslEngineProviderFactory sslEngineProviderFactory, SslPolicy clientSslPolicy, SslPolicy serverSslPolicy,
            BiConsumer<TestProbe,TestProbe> verify ) throws Exception
    {
        SSLEngineProvider clientSslProvider = sslEngineProviderFactory.newProvider( clientSslPolicy );
        ActorSystem clientActorSystem = createActorSystem( "client", clientSslProvider );

        SSLEngineProvider serverSslProvider = sslEngineProviderFactory.newProvider( serverSslPolicy );
        ActorSystem serverActorSystem = createActorSystem( "server", serverSslProvider );

        TestProbe serverMsgProbe = new TestProbe( serverActorSystem );
        TestProbe clientLogProbe = new TestProbe( clientActorSystem );
        clientActorSystem.eventStream().subscribe( clientLogProbe.ref(), Logging.Warning.class );

        ActorRef server = serverActorSystem.actorOf( Forwarder.props( serverActorSystem.actorSelection( serverMsgProbe.ref().path() ) ) );
        String serverPath = server.path()
                .toStringWithAddress( Address.apply( "akka", serverActorSystem.name(), "localhost",
                        serverActorSystem.settings().config().getInt( "akka.remote.artery.canonical.port" ) ) );
        ActorRef client = clientActorSystem.actorOf( Forwarder.props( clientActorSystem.actorSelection( serverPath ) ) );

        client.tell( MSG, ActorRef.noSender() );

        try
        {
            verify.accept( serverMsgProbe, clientLogProbe );
        }
        finally
        {
            var cleanup = Stream.of( clientActorSystem, serverActorSystem )
                    .peek( ActorSystem::terminate )
                    .map( ActorSystem::getWhenTerminated )
                    .map( CompletionStage::toCompletableFuture )
                    .toArray( CompletableFuture<?>[]::new );

            CompletableFuture.allOf( cleanup ).get( 60, TimeUnit.SECONDS );
        }
    }

    private void accept( TestProbe serverMsgProbe, TestProbe clientLogProbe )
    {
        serverMsgProbe.expectMsg( TIMEOUT, MSG );
        clientLogProbe.expectNoMessage();
    }

    private void decline( TestProbe serverMsgProbe, TestProbe clientLogProbe )
    {
        serverMsgProbe.expectNoMessage();
        Logging.LogEvent log = clientLogProbe.expectMsgClass( TIMEOUT, Logging.Warning.class );
        String message = log.message().toString();
        assertThat( message, anyOf( containsString( "SSLHandshakeException" ), containsString( "SSLException" ), containsString( "StreamTcpException" ) ) );
    }

    private static class Forwarder extends AbstractLoggingActor
    {
        private ActorSelection remote;

        Forwarder( ActorSelection remote )
        {
            this.remote = remote;
        }

        static Props props( ActorSelection remote )
        {
            return Props.create( Forwarder.class, () -> new Forwarder( remote ) );
        }

        @Override
        public Receive createReceive()
        {
            return ReceiveBuilder.create()
                    .match( Object.class, msg ->
                    {
                        log().info( "Forwarding '{}' to {}", msg, remote );
                        remote.forward( msg, getContext() );
                    } )
                    .build();
        }
    }

    private static class TestSSLEngineProvider extends AkkaDiscoverySSLEngineProvider
    {
        TestSSLEngineProvider( SslPolicy sslPolicy )
        {
            super( sslPolicy );
        }

        /**
         * @param hostname ignored, with 0.0.0.0 instead. This has been observed, and has caused the combination
         * of client auth and hostname verification to fail.
         */
        @Override
        public SSLEngine createServerSSLEngine( String hostname, int port )
        {
            return super.createServerSSLEngine( "0.0.0.0", port );
        }
    }

    private enum SslEngineProviderFactory
    {
        AKKA_SSL_ENGINE( AkkaDiscoverySSLEngineProvider::new ),
        TEST_SSL_ENGINE( TestSSLEngineProvider::new );

        final Function<SslPolicy,SSLEngineProvider> factory;

        SslEngineProviderFactory( Function<SslPolicy,SSLEngineProvider> factory )
        {
            this.factory = factory;
        }

        SSLEngineProvider newProvider( SslPolicy sslPolicy )
        {
            return factory.apply( sslPolicy );
        }
    }
}
