/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.bolt;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.neo4j.configuration.BoltConnector;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.configuration.ssl.SslPolicyConfig;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.ssl.SecureClient;
import org.neo4j.ssl.SslContextFactory;
import org.neo4j.ssl.SslResource;
import org.neo4j.test.TestGraphDatabaseFactory;
import org.neo4j.test.rule.TestDirectory;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.neo4j.ssl.SslContextFactory.SslParameters.protocols;
import static org.neo4j.ssl.SslContextFactory.makeSslPolicy;
import static org.neo4j.ssl.SslResourceBuilder.selfSignedKeyId;
import static org.neo4j.test.PortUtils.getBoltPort;

@RunWith( Parameterized.class )
public class BoltTlsIT
{
    @Rule
    public final TestDirectory testDirectory = TestDirectory.testDirectory();
    private final LogProvider logProvider = NullLogProvider.getInstance();

    private SslPolicyConfig sslPolicy = new SslPolicyConfig( "bolt" );

    private GraphDatabaseAPI db;
    private SslResource sslResource;

    private BoltConnector bolt = new BoltConnector( "bolt" );

    @Before
    public void setup() throws IOException
    {
        File sslObjectsDir = new File( testDirectory.storeDir(), "certificates" );
        assertTrue( sslObjectsDir.mkdirs() );

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
        db = (GraphDatabaseAPI) new TestGraphDatabaseFactory()
                .newImpermanentDatabaseBuilder( testDirectory.databaseDir() )
                .setConfig( bolt.enabled, "true" )
                .setConfig( bolt.listen_address, "localhost:0" )
                .setConfig( GraphDatabaseSettings.bolt_ssl_policy, "bolt" )
                .setConfig( sslPolicy.allow_key_generation, "true" )
                .setConfig( sslPolicy.base_directory, "certificates" )
                .setConfig( sslPolicy.tls_versions, setup.boltTlsVersions )
                .setConfig( sslPolicy.client_auth, "none" )
                .setConfig( sslPolicy.verify_hostname, "false" )
                .newGraphDatabase();
    }

    @After
    public void teardown()
    {
        if ( db != null )
        {
            db.shutdown();
        }
    }

    @Test
    public void shouldRespectProtocolSelection() throws Exception
    {
        // given
        SslContextFactory.SslParameters params = protocols( setup.clientTlsVersions ).ciphers();
        SecureClient client = new SecureClient( makeSslPolicy( sslResource, params ) );

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
