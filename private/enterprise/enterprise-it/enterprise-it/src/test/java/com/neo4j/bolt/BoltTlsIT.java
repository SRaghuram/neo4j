/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.bolt;

import com.neo4j.ssl.SecureClient;
import com.neo4j.ssl.SslContextFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.neo4j.configuration.connectors.BoltConnector;
import org.neo4j.configuration.connectors.BoltConnector.EncryptionLevel;
import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.configuration.ssl.ClientAuth;
import org.neo4j.configuration.ssl.SslPolicyConfig;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.ssl.SslResource;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.rule.TestDirectory;

import static com.neo4j.ssl.SslContextFactory.SslParameters.protocols;
import static com.neo4j.ssl.SslContextFactory.makeSslPolicy;
import static java.nio.file.Files.createDirectories;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.configuration.ssl.SslPolicyScope.BOLT;
import static org.neo4j.ssl.SslResourceBuilder.selfSignedKeyId;
import static org.neo4j.test.PortUtils.getBoltPort;

@RunWith( Parameterized.class )
public class BoltTlsIT
{
    @Rule
    public final TestDirectory testDirectory = TestDirectory.testDirectory();
    private final LogProvider logProvider = NullLogProvider.getInstance();

    private SslPolicyConfig sslPolicy = SslPolicyConfig.forScope( BOLT );

    private GraphDatabaseAPI db;
    private SslResource sslResource;

    private DatabaseManagementService managementService;

    @Before
    public void setup() throws IOException
    {
        Path sslObjectsDir = testDirectory.homePath().resolve( "certificates" );
        createDirectories( sslObjectsDir );

        sslResource = selfSignedKeyId( 0 ).trustKeyId( 0 ).install( sslObjectsDir );

        createAndStartDb();
    }

    static class TestSetup
    {
        private final String clientTlsVersions;
        private final String boltTlsVersions;
        private final boolean shouldSucceed;

        TestSetup( String clientTlsVersions, String boltTlsVersion, boolean shouldSucceed )
        {
            this.clientTlsVersions = clientTlsVersions;
            this.boltTlsVersions = boltTlsVersion;
            this.shouldSucceed = shouldSucceed;
        }

        @Override
        public String toString()
        {
            return "TestSetup{"
                    + "clientTlsVersions='" + clientTlsVersions + '\''
                    + ", boltTlsVersions='" + boltTlsVersions + '\''
                    + ", shouldSucceed=" + shouldSucceed + '}';
        }
    }

    @Parameterized.Parameters( name = "{0}" )
    public static Object[] params()
    {
        return new TestSetup[]{
                new TestSetup( "TLSv1.1", "TLSv1.2", false ),
                new TestSetup( "TLSv1.2", "TLSv1.1", false ),
                new TestSetup( "TLSv1", "TLSv1.1", false ),
                new TestSetup( "TLSv1.1", "TLSv1.2", false ),

                new TestSetup( "TLSv1", "TLSv1", true ),
                new TestSetup( "TLSv1.1", "TLSv1.1", true ),
                new TestSetup( "TLSv1.2", "TLSv1.2", true ),

                new TestSetup( "SSLv3,TLSv1", "TLSv1.1,TLSv1.2", false ),
                new TestSetup( "TLSv1.1,TLSv1.2", "TLSv1.1,TLSv1.2", true ),
        };
    }

    @Parameter
    public TestSetup setup;

    private void createAndStartDb()
    {
        managementService = new TestDatabaseManagementServiceBuilder( testDirectory.homePath() ).impermanent()
                .setConfig( BoltConnector.enabled, true )
                .setConfig( BoltConnector.listen_address, new SocketAddress( "localhost", 0 ) )
                .setConfig( BoltConnector.advertised_address, new SocketAddress( 0 ) )
                .setConfig( BoltConnector.encryption_level, EncryptionLevel.OPTIONAL )
                .setConfig( sslPolicy.enabled, true )
                .setConfig( sslPolicy.base_directory, Path.of( "certificates" ) )
                .setConfig( sslPolicy.tls_versions, Arrays.asList( setup.boltTlsVersions.split( "," ) ) )
                .setConfig( sslPolicy.client_auth, ClientAuth.NONE )
                .setConfig( sslPolicy.verify_hostname, false ).build();
        db = (GraphDatabaseAPI) managementService.database( DEFAULT_DATABASE_NAME );
    }

    @After
    public void teardown()
    {
        if ( db != null )
        {
            managementService.shutdown();
        }
    }

    @Test
    public void shouldRespectProtocolSelection() throws Exception
    {
        // given
        SslContextFactory.SslParameters params = protocols( setup.clientTlsVersions.split( "," ) ).ciphers();
        SecureClient client = new SecureClient( makeSslPolicy( sslResource, params, BOLT ) );

        // when
        client.connect( getBoltPort( db ) );

        // then
        try
        {
            assertTrue( client.sslHandshakeFuture().get( 1, TimeUnit.MINUTES ).isActive() );
        }
        catch ( ExecutionException e )
        {
            assertFalse( setup.shouldSucceed );
        }
    }
}
