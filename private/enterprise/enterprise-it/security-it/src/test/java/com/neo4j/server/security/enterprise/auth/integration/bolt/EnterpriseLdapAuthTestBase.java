/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.security.enterprise.auth.integration.bolt;

import com.neo4j.server.security.enterprise.auth.EnterpriseAuthAndUserManager;
import com.neo4j.test.rule.EnterpriseDbmsRule;
import org.apache.directory.server.core.integ.AbstractLdapTestUnit;
import org.junit.After;
import org.junit.Rule;
import org.junit.rules.RuleChain;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.Reader;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.URL;
import java.util.Collections;
import java.util.Map;

import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.configuration.connectors.BoltConnector;
import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.internal.kernel.api.security.AuthSubject;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.api.exceptions.InvalidArgumentsException;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.TestDirectory;

import static com.neo4j.server.security.enterprise.auth.integration.bolt.DriverAuthHelper.boltUri;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.neo4j.configuration.connectors.BoltConnector.EncryptionLevel.DISABLED;
import static org.neo4j.server.security.auth.SecurityTestUtils.password;

public abstract class EnterpriseLdapAuthTestBase extends AbstractLdapTestUnit
{
    private final TestDirectory testDirectory = TestDirectory.testDirectory();

    DbmsRule dbRule = new EnterpriseDbmsRule( testDirectory ).startLazily();

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
              .withSetting( BoltConnector.listen_address, new SocketAddress(  InetAddress.getLoopbackAddress().getHostAddress(), 0 ) );
        dbRule.withSettings( getSettings() );
        dbRule.withSettings( settings );
        dbRule.ensureStarted();
        boltUri = boltUri( dbRule );
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
        { /* Ignore, Port not reachable */
        }
        return false;
    }

    void assertSecurityLogContains( String message ) throws IOException
    {
        FileSystemAbstraction fileSystem = testDirectory.getFileSystem();
        File workingDirectory = testDirectory.directory();
        File logFile = new File( workingDirectory, "logs/security.log" );

        Reader reader = fileSystem.openAsReader( logFile, UTF_8 );
        BufferedReader bufferedReader = new BufferedReader( reader );
        String line;
        boolean foundError = false;

        while ( (line = bufferedReader.readLine()) != null )
        {
            if ( line.contains( message ) )
            {
                foundError = true;
            }
        }
        bufferedReader.close();
        reader.close();

        assertThat( "Security log should contain message '" + message + "'", foundError );
    }

    void assertSecurityLogDoesNotContain( String message ) throws IOException
    {
        FileSystemAbstraction fileSystem = testDirectory.getFileSystem();
        File workingDirectory = testDirectory.directory();
        File logFile = new File( workingDirectory, "logs/security.log" );

        Reader reader = fileSystem.openAsReader( logFile, UTF_8 );
        BufferedReader bufferedReader = new BufferedReader( reader );
        String line;

        while ( (line = bufferedReader.readLine()) != null )
        {
            assertThat( "Security log should not contain message '" + message + "'",
                    !line.contains( message ) );
        }
        bufferedReader.close();
        reader.close();
    }

    void createNativeUser( String username, String password, String... roles ) throws IOException, InvalidArgumentsException
    {
        EnterpriseAuthAndUserManager authManager =
                dbRule.resolveDependency( EnterpriseAuthAndUserManager.class );

        authManager.getUserManager( AuthSubject.AUTH_DISABLED, true )
                .newUser( username, password( password ), false );

        for ( String role : roles )
        {
            authManager.getUserManager( AuthSubject.AUTH_DISABLED, true )
                    .addRoleToUser( role, username );
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
            File keyStoreFile = new File( url.getFile() );
            String keyStorePath = keyStoreFile.getAbsolutePath();

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
