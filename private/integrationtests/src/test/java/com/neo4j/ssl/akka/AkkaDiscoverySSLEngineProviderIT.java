/*
 * Copyright (c) 2002-2018 "Neo4j,"
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
import akka.actor.CoordinatedShutdown;
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
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import scala.concurrent.duration.FiniteDuration;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.BiConsumer;
import java.util.function.Function;
import javax.net.ssl.SSLEngine;

import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.configuration.ssl.SslPolicyLoader;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.ports.allocation.PortAuthority;
import org.neo4j.ssl.SslPolicy;
import org.neo4j.ssl.SslResource;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.test.rule.fs.DefaultFileSystemRule;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.either;
import static org.junit.Assert.assertThat;
import static org.neo4j.ssl.HostnameVerificationHelper.POLICY_NAME;
import static org.neo4j.ssl.HostnameVerificationHelper.SSL_POLICY_CONFIG;
import static org.neo4j.ssl.HostnameVerificationHelper.aConfig;
import static org.neo4j.ssl.HostnameVerificationHelper.trust;
import static org.neo4j.ssl.SslContextFactory.SslParameters.protocols;
import static org.neo4j.ssl.SslContextFactory.makeSslPolicy;
import static org.neo4j.ssl.SslResourceBuilder.caSignedKeyId;
import static org.neo4j.ssl.SslResourceBuilder.selfSignedKeyId;

@RunWith( Parameterized.class )
public class AkkaDiscoverySSLEngineProviderIT
{
    private static final String MSG = "When in doubt, burn it to the ground and start from scratch";

    private static final String TLSv11 = "TLSv1.1";
    private static final String TLSv12 = "TLSv1.2";
    private static final String NEW_CIPHER_A = "TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA";
    private static final String NEW_CIPHER_B = "TLS_RSA_WITH_AES_128_CBC_SHA256";

    private static final int UNRELATED_ID = 5; // SslContextFactory requires us to trust something

    private static final FiniteDuration TIMEOUT = new FiniteDuration( 10L, TimeUnit.SECONDS );

    @Rule
    public TestDirectory testDir = TestDirectory.testDirectory();

    @Rule
    public DefaultFileSystemRule fsRule = new DefaultFileSystemRule();

    @Parameterized.Parameter
    public Function<SslPolicy,SSLEngineProvider> sslEngineProviderFactory;

    @Parameterized.Parameters
    public static Collection<Function<SslPolicy, SSLEngineProvider>> data()
    {
        return Arrays.asList( AkkaDiscoverySSLEngineProvider::new, TestSSLEngineProvider::new );
    }

    @Test
    public void shouldConnectWithMutualTrust() throws Throwable
    {
        SslResource sslServerResource = selfSignedKeyId( 0 ).trustKeyId( 1 ).install( testDir.directory( "server" ) );
        SslResource sslClientResource = selfSignedKeyId( 1 ).trustKeyId( 0 ).install( testDir.directory( "client" ) );

        testConnection( sslClientResource, sslServerResource, this::accept );
    }

    @Test
    public void shouldConnectWithMutualTrustViaCA() throws Throwable
    {
        SslResource sslServerResource = caSignedKeyId( 0 ).trustSignedByCA().install( testDir.directory( "server" ) );
        SslResource sslClientResource = caSignedKeyId( 1 ).trustSignedByCA().install( testDir.directory( "client" ) );

        testConnection( sslClientResource, sslServerResource, this::accept );
    }

    @Test
    public void shouldNotConnectWithUntrustedClient() throws Throwable
    {
        SslResource sslClientResource = selfSignedKeyId( 1 ).trustKeyId( 0 ).install( testDir.directory( "client" ) );
        SslResource sslServerResource = selfSignedKeyId( 0 ).trustKeyId( UNRELATED_ID ).install( testDir.directory( "server" ) );

        testConnection( sslClientResource, sslServerResource, this::decline );
    }

    @Test
    public void shouldNotConnectWithUntrustedServer() throws Throwable
    {
        SslResource sslClientResource = selfSignedKeyId( 0 ).trustKeyId( UNRELATED_ID ).install( testDir.directory( "client" ) );
        SslResource sslServerResource = selfSignedKeyId( 1 ).trustKeyId( 0 ).install( testDir.directory( "server" ) );

        testConnection( sslClientResource, sslServerResource, this::decline );
    }

    @Test
    public void shouldNotConnectWhenTrustedByCAAndServerRevoked() throws Throwable
    {
        SslResource sslServerResource = caSignedKeyId( 0 ).trustSignedByCA().install( testDir.directory( "server" ) );
        SslResource sslClientResource = caSignedKeyId( 1 ).trustSignedByCA().revoke( 0 ).install( testDir.directory( "client" ) );

        testConnection( sslClientResource, sslServerResource, this::decline );
    }

    @Test
    public void shouldNotConnectWhenTrustedByCAAndClientRevoked() throws Throwable
    {
        SslResource sslServerResource = caSignedKeyId( 0 ).trustSignedByCA().revoke( 1 ).install( testDir.directory( "server" ) );
        SslResource sslClientResource = caSignedKeyId( 1 ).trustSignedByCA().install( testDir.directory( "client" ) );

        testConnection( sslClientResource, sslServerResource, this::decline );
    }

    @Test
    public void shouldConnectIfProtocolsInCommon() throws Throwable
    {
        SslResource sslServerResource = selfSignedKeyId( 0 ).trustKeyId( 1 ).install( testDir.directory( "server" ) );
        SslResource sslClientResource = selfSignedKeyId( 1 ).trustKeyId( 0 ).install( testDir.directory( "client" ) );
        SslPolicy serverSslPolicy = makeSslPolicy( sslServerResource, protocols( TLSv12 ).ciphers() );
        SslPolicy clientSslPolicy = makeSslPolicy( sslClientResource, protocols( TLSv12 ).ciphers() );

        testConnection( clientSslPolicy, serverSslPolicy, this::accept );
    }

    @Test
    public void shouldNotConnectIfNoProtocolsInCommon() throws Throwable
    {
        SslResource sslServerResource = selfSignedKeyId( 0 ).trustKeyId( 1 ).install( testDir.directory( "server" ) );
        SslResource sslClientResource = selfSignedKeyId( 1 ).trustKeyId( 0 ).install( testDir.directory( "client" ) );
        SslPolicy serverSslPolicy = makeSslPolicy( sslServerResource, protocols( TLSv12 ).ciphers() );
        SslPolicy clientSslPolicy = makeSslPolicy( sslClientResource, protocols( TLSv11 ).ciphers() );

        testConnection( clientSslPolicy, serverSslPolicy, this::decline );
    }

    @Test
    public void shouldConnectIfCiphersInCommon() throws Throwable
    {
        SslResource sslServerResource = selfSignedKeyId( 0 ).trustKeyId( 1 ).install( testDir.directory( "server" ) );
        SslResource sslClientResource = selfSignedKeyId( 1 ).trustKeyId( 0 ).install( testDir.directory( "client" ) );
        SslPolicy serverSslPolicy = makeSslPolicy( sslServerResource, protocols().ciphers( NEW_CIPHER_A ) );
        SslPolicy clientSslPolicy = makeSslPolicy( sslClientResource, protocols().ciphers( NEW_CIPHER_A ) );

        testConnection( clientSslPolicy, serverSslPolicy, this::accept );
    }

    @Test
    public void shouldNotConnectIfNoCiphersInCommon() throws Throwable
    {
        SslResource sslServerResource = selfSignedKeyId( 0 ).trustKeyId( 1 ).install( testDir.directory( "server" ) );
        SslResource sslClientResource = selfSignedKeyId( 1 ).trustKeyId( 0 ).install( testDir.directory( "client" ) );
        SslPolicy serverSslPolicy = makeSslPolicy( sslServerResource, protocols().ciphers( NEW_CIPHER_A ) );
        SslPolicy clientSslPolicy = makeSslPolicy( sslClientResource, protocols().ciphers( NEW_CIPHER_B ) );

        testConnection( clientSslPolicy, serverSslPolicy, this::decline );
    }

    @Test
    public void shouldNotConnectIfInvalidCommonNameOnServer() throws Throwable
    {
        Config serverConfig = aConfig( "invalid", testDir );

        Config clientConfig = aConfig( "localhost", testDir );

        trust( serverConfig, clientConfig );
        trust( clientConfig, serverConfig );

        SslPolicy serverPolicy = SslPolicyLoader.create( serverConfig, NullLogProvider.getInstance() ).getPolicy( POLICY_NAME );
        SslPolicy clientPolicy = SslPolicyLoader.create( clientConfig, NullLogProvider.getInstance() ).getPolicy( POLICY_NAME );

        testConnection( clientPolicy, serverPolicy, this::decline );
    }

    @Test
    public void shouldConnectIfValidCommonName() throws Throwable
    {
        Config serverConfig = aConfig( "localhost", testDir );

        Config clientConfig = aConfig( "localhost", testDir );

        trust( serverConfig, clientConfig );
        trust( clientConfig, serverConfig );

        SslPolicy serverPolicy = SslPolicyLoader.create( serverConfig, NullLogProvider.getInstance() ).getPolicy( POLICY_NAME );
        SslPolicy clientPolicy = SslPolicyLoader.create( clientConfig, NullLogProvider.getInstance() ).getPolicy( POLICY_NAME );

        testConnection( clientPolicy, serverPolicy, this::accept );
    }

    @Test
    public void shouldConnectIfLegacyPolicyRegardlessOfHostname() throws Throwable
    {
        Config serverConfig = aConfig( "invalid-server", testDir );

        Config clientConfig = aConfig( "invalid-client", testDir );

        trust( serverConfig, clientConfig );
        trust( clientConfig, serverConfig );

        SslPolicy serverPolicy = SslPolicyLoader.create( serverConfig, NullLogProvider.getInstance() ).getPolicy( "legacy" );
        SslPolicy clientPolicy = SslPolicyLoader.create( clientConfig, NullLogProvider.getInstance() ).getPolicy( "legacy" );

        testConnection( clientPolicy, serverPolicy, this::accept );
    }

    @Test
    public void shouldConnectWithHostnameVerificationAndClientAuth() throws Throwable
    {
        Config serverConfig = aConfig( "localhost", testDir );
        serverConfig.augment( SSL_POLICY_CONFIG.client_auth, "require" );

        Config clientConfig = aConfig( "localhost", testDir );
        clientConfig.augment( SSL_POLICY_CONFIG.client_auth, "require" );

        trust( serverConfig, clientConfig );
        trust( clientConfig, serverConfig );

        SslPolicy serverPolicy = SslPolicyLoader.create( serverConfig, NullLogProvider.getInstance() ).getPolicy( POLICY_NAME );
        SslPolicy clientPolicy = SslPolicyLoader.create( clientConfig, NullLogProvider.getInstance() ).getPolicy( POLICY_NAME );

        testConnection( clientPolicy, serverPolicy, this::accept );
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

    private void testConnection( SslResource clientSslResource, SslResource serverSslResource, BiConsumer<TestProbe,TestProbe> verify )
            throws InterruptedException, ExecutionException, TimeoutException
    {
        testConnection( makeSslPolicy( clientSslResource ), makeSslPolicy( serverSslResource ), verify );
    }

    private void testConnection( SslPolicy clientSslPolicy, SslPolicy serverSslPolicy, BiConsumer<TestProbe,TestProbe> verify )
            throws InterruptedException, ExecutionException, TimeoutException
    {
        SSLEngineProvider clientSslProvider = sslEngineProviderFactory.apply( clientSslPolicy );
        ActorSystem clientActorSystem = createActorSystem( "client", clientSslProvider );

        SSLEngineProvider serverSslProvider = sslEngineProviderFactory.apply( serverSslPolicy );
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
            CoordinatedShutdown.get( clientActorSystem ).runAll().toCompletableFuture().get( 10, TimeUnit.SECONDS );
            CoordinatedShutdown.get( serverActorSystem ).runAll().toCompletableFuture().get( 10, TimeUnit.SECONDS );
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
        assertThat( log.message().toString(), either( containsString( "SSLHandshakeException" ) ).or( containsString( "SSLException" ) ) );
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
}
