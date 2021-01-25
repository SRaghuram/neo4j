/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.security.enterprise.auth.integration.bolt;

import com.neo4j.test.rule.EnterpriseDbmsRule;
import org.apache.commons.io.LineIterator;
import org.apache.directory.server.core.integ.AbstractLdapTestUnit;
import org.junit.After;
import org.junit.Rule;
import org.junit.rules.RuleChain;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.URL;
import java.nio.file.Path;
import java.util.Collections;
import java.util.Map;

import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.configuration.connectors.BoltConnector;
import org.neo4j.configuration.connectors.ConnectorPortRegister;
import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.TestDirectory;

import static com.neo4j.server.security.enterprise.auth.integration.bolt.DriverAuthHelper.boltUri;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.commons.io.IOUtils.lineIterator;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;
import static org.neo4j.configuration.connectors.BoltConnector.EncryptionLevel.DISABLED;

public abstract class EnterpriseLdapAuthTestBase extends AbstractLdapTestUnit
{
    private final TestDirectory testDirectory = TestDirectory.testDirectory();

    DbmsRule dbRule = new EnterpriseDbmsRule( testDirectory ).startLazily();

    private GraphDatabaseFacade systemDb;

    @Rule
    public RuleChain chain = RuleChain.outerRule( testDirectory ).around( dbRule );

    String boltUri;

    void startDatabase()
    {
        startDatabaseWithSettings( Collections.emptyMap() );
    }

    void startDatabaseWithSettings( Map<Setting<?>,Object> settings )
    {
        dbRule.withSetting( GraphDatabaseSettings.auth_enabled, true )
              .withSetting( BoltConnector.enabled, true )
              .withSetting( BoltConnector.encryption_level, DISABLED )
              .withSetting( BoltConnector.listen_address, new SocketAddress( InetAddress.getLoopbackAddress().getHostAddress(), 0 ) );
        dbRule.withSettings( getSettings() );
        dbRule.withSettings( settings );
        dbRule.ensureStarted();
        boltUri = boltUri( dbRule.resolveDependency( ConnectorPortRegister.class ) );
        systemDb = (GraphDatabaseFacade) dbRule.getManagementService().database( SYSTEM_DATABASE_NAME );
        try ( org.neo4j.graphdb.Transaction tx = dbRule.beginTx() )
        {
            // create a node to be able to assert that access without other privileges sees empty graph
            tx.createNode();
            tx.commit();
        }
    }

    @After
    public void teardown()
    {
        dbRule.shutdown();
    }

    protected abstract Map<Setting<?>,Object> getSettings();

    void checkIfLdapServerIsReachable( String host, int port )
    {
        if ( !portIsReachable( host, port, 10000 ) )
        {
            throw new IllegalStateException( "Ldap Server is not reachable on " + host + ":" + port + "." );
        }
    }

    private boolean portIsReachable( String host, int port, int timeOutMS )
    {
        try ( Socket serverSocket = new Socket() )
        {
            serverSocket.connect( new InetSocketAddress( host, port ), timeOutMS );
            return true;
        }
        catch ( final IOException e )
        {
            // Ignore, Port not reachable
        }
        return false;
    }

    void assertSecurityLogContains( String message ) throws IOException
    {
        FileSystemAbstraction fileSystem = testDirectory.getFileSystem();
        Path workingDirectory = testDirectory.homePath();
        Path logFile = workingDirectory.resolve( "logs/security.log" );

        boolean foundError = false;
        try ( var reader = fileSystem.openAsReader( logFile, UTF_8 ) )
        {
            var lineReader = lineIterator( reader );

            while ( lineReader.hasNext() )
            {
                var line = lineReader.nextLine();
                if ( line.contains( message ) )
                {
                    foundError = true;
                }
            }
        }

        assertThat( "Security log should contain message '" + message + "'", foundError );
    }

    void assertSecurityLogDoesNotContain( String message ) throws IOException
    {
        FileSystemAbstraction fileSystem = testDirectory.getFileSystem();
        Path workingDirectory = testDirectory.homePath();
        Path logFile = workingDirectory.resolve( "logs/security.log" );

        try ( var reader = fileSystem.openAsReader( logFile, UTF_8 ) )
        {
            LineIterator lineIterator = lineIterator( reader );

            while ( lineIterator.hasNext() )
            {
                var line = lineIterator.nextLine();
                assertThat( "Security log should not contain message '" + message + "'", !line.contains( message ) );
            }
        }
    }

    @SuppressWarnings( "SameParameterValue" )
    void createRole( String roleName )
    {
        executeOnSystem( String.format( "CREATE ROLE %s", roleName ) );
    }

    void createNativeUser( String username, String password, String... roles )
    {
        executeOnSystem( String.format( "CREATE USER %s SET PASSWORD '%s' CHANGE NOT REQUIRED", username, password ) );

        for ( String role : roles )
        {
            executeOnSystem( String.format( "GRANT ROLE %s TO %s", role, username ) );
        }
    }

    void executeOnSystem( String query )
    {
        try ( Transaction tx = systemDb.beginTx() )
        {
            tx.execute( query );
            tx.commit();
        }
    }

    //-------------------------------------------------------------------------
    // TLS helper
    static class EmbeddedTestCertificates implements AutoCloseable
    {
        private static final String KEY_STORE = "javax.net.ssl.keyStore";
        private static final String KEY_STORE_PASSWORD = "javax.net.ssl.keyStorePassword";
        private static final String TRUST_STORE = "javax.net.ssl.trustStore";
        private static final String TRUST_STORE_PASSWORD = "javax.net.ssl.trustStorePassword";

        private final String keyStore = System.getProperty( KEY_STORE );
        private final String keyStorePassword = System.getProperty( KEY_STORE_PASSWORD );
        private final String trustStore = System.getProperty( TRUST_STORE );
        private final String trustStorePassword = System.getProperty( TRUST_STORE_PASSWORD );

        EmbeddedTestCertificates()
        {
            URL url = getClass().getResource( "/neo4j_ldap_test_keystore.jks" );
            Path keyStoreFile = Path.of( url.getFile() );
            String keyStorePath = keyStoreFile.toAbsolutePath().toString();

            System.setProperty( KEY_STORE, keyStorePath );
            System.setProperty( KEY_STORE_PASSWORD, "secret" );
            System.setProperty( TRUST_STORE, keyStorePath );
            System.setProperty( TRUST_STORE_PASSWORD, "secret" );
        }

        @Override
        public void close()
        {
            resetProperty( KEY_STORE, keyStore );
            resetProperty( KEY_STORE_PASSWORD, keyStorePassword );
            resetProperty( TRUST_STORE, trustStore );
            resetProperty( TRUST_STORE_PASSWORD, trustStorePassword );
        }

        private void resetProperty( String property, String value )
        {
            if ( value == null )
            {
                System.clearProperty( property );
            }
            else
            {
                System.setProperty( property, value );
            }
        }
    }
}
