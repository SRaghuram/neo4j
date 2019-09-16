/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.ssl;

import org.apache.commons.lang3.SystemUtils;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.configuration.ssl.ClientAuth;
import org.neo4j.configuration.ssl.JksSslPolicyConfig;
import org.neo4j.configuration.ssl.KeyStoreSslPolicyConfig;
import org.neo4j.configuration.ssl.Pkcs12SslPolicyConfig;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.ssl.SslPolicy;
import org.neo4j.ssl.config.SslPolicyLoader;
import org.neo4j.string.SecureString;
import org.neo4j.test.rule.TestDirectory;

import static com.neo4j.ssl.PemSslPolicyLoaderIT.clientCanCommunicateWithServer;
import static com.neo4j.ssl.PemSslPolicyLoaderIT.clientCannotCommunicateWithServer;
import static org.junit.Assert.fail;
import static org.neo4j.configuration.ssl.SslPolicyScope.TESTING;

@RunWith( Parameterized.class )
public class KeyStoreSslPolicyLoaderIT
{
    @Rule
    public TestDirectory testDirectory = TestDirectory.testDirectory();

    private static final String TLSV1_1 = "TLSv1.1";
    private static final String TLSV1_2 = "TLSv1.2";
    private static final String TLSV1_3 = "TLSv1.3";
    private static final String TRUSTED_ALIAS = "trusted";
    private final KeyStoreSslPolicyConfig SSL_POLICY_CONFIG;
    private static final String KEYPAIR_ALIAS = "test";
    private static final String KEYSTORE = "keystore";
    private static final String KEYSTORE_PASS = "foobar";
    private static final String TRUSTSTORE = "truststore";
    private static final String TRUSTSTORE_PASS = "bazquux";
    private static final String CA_ALIAS = "CA";

    private static final String JKS = "jks";
    private static final String PKCS12 = "pkcs12";

    private final String keyPass;
    private final String format;

    private File serverDir;
    private File clientDir;

    @Parameterized.Parameters( name = "{0}" )
    public static Collection<String> data()
    {
        return Arrays.asList( JKS, PKCS12 );
    }

    public KeyStoreSslPolicyLoaderIT( String format )
    {
        String keyPass = null;
        KeyStoreSslPolicyConfig policyConfig = null;
        if ( format.equals( PKCS12 ) )
        {
            keyPass = KEYSTORE_PASS;
            policyConfig = Pkcs12SslPolicyConfig.forScope( TESTING );
        }
        else if ( format.equals( JKS ) )
        {
            keyPass = "barquux";
            policyConfig = JksSslPolicyConfig.forScope( TESTING );

        }
        else
        {
            fail( "Unknown format: " + format );
        }

        this.SSL_POLICY_CONFIG = policyConfig;
        this.keyPass = keyPass;
        this.format = format;
    }

    @Before
    public void setUp()
    {
        assumeFormatSupported();

        UUID random = UUID.randomUUID();
        this.serverDir = testDirectory.directory( "base_directory_server_" + random );
        this.clientDir = testDirectory.directory( "base_directory_client_" + random );
    }

    @Test
    public void shouldConnectWithServerTrustedByClient() throws Throwable
    {
        // given server
        Config serverConfig = aConfig( serverDir, "localhost" ).build();

        // and client
        Config clientConfig = aConfig( clientDir, "localhost" ).build();

        // and trust
        copyServerCertToClientTrustedCertEntry( clientDir, serverDir, KEYSTORE, KEYSTORE_PASS );

        // and setup
        SslPolicy serverPolicy = SslPolicyLoader.create( serverConfig, NullLogProvider.getInstance() ).getPolicy( TESTING );
        SslPolicy clientPolicy = SslPolicyLoader.create( clientConfig, NullLogProvider.getInstance() ).getPolicy( TESTING );
        SecureServer secureServer = new SecureServer( serverPolicy );
        secureServer.start();
        SecureClient secureClient = new SecureClient( clientPolicy );

        // then
        clientCanCommunicateWithServer( secureClient, secureServer );
    }

    @Test
    public void shouldConnectWithServerTrustedByClientWithSeparateTrustStoreFile() throws Throwable
    {
        // given server
        Config serverConfig = aConfig( serverDir, "localhost" ).build();

        // and client
        Path trustStoreFile = clientDir.toPath().resolve( TRUSTSTORE );
        Config clientConfig = aConfig( clientDir, "localhost" )
                .set( SSL_POLICY_CONFIG.truststore, trustStoreFile )
                .set( SSL_POLICY_CONFIG.truststore_pass, new SecureString( TRUSTSTORE_PASS ) )
                .build();

        // and trust
        copyServerCertToClientTrustedCertEntry( clientDir, serverDir, TRUSTSTORE, TRUSTSTORE_PASS );

        // and setup
        SslPolicy serverPolicy = SslPolicyLoader.create( serverConfig, NullLogProvider.getInstance() ).getPolicy( TESTING );
        SslPolicy clientPolicy = SslPolicyLoader.create( clientConfig, NullLogProvider.getInstance() ).getPolicy( TESTING );
        SecureServer secureServer = new SecureServer( serverPolicy );
        secureServer.start();
        SecureClient secureClient = new SecureClient( clientPolicy );

        // then
        clientCanCommunicateWithServer( secureClient, secureServer );
    }

    @Test
    public void shouldConnectWithCATrustedCert() throws Throwable
    {
        // given server
        Config serverConfig = aConfig( serverDir, "localhost" ).build();

        // and client
        Config clientConfig = aConfig( clientDir, "localhost" ).build();

        // and trust
        signServerCertWithCAAndCopyCACertToClientTrustedCertEntry( serverDir, clientDir );

        // and setup
        SslPolicy serverPolicy = SslPolicyLoader.create( serverConfig, NullLogProvider.getInstance() ).getPolicy( TESTING );
        SslPolicy clientPolicy = SslPolicyLoader.create( clientConfig, NullLogProvider.getInstance() ).getPolicy( TESTING );
        SecureServer secureServer = new SecureServer( serverPolicy );
        secureServer.start();
        SecureClient secureClient = new SecureClient( clientPolicy );

        // then
        clientCanCommunicateWithServer( secureClient, secureServer );
    }

    @Test
    public void shouldNotConnectWithoutTrustedCerts() throws Throwable
    {
        // given server
        Config serverConfig = aConfig( serverDir, "localhost" ).build();

        // and client
        Config clientConfig = aConfig( clientDir, "localhost" ).build();

        // and setup
        SslPolicy serverPolicy = SslPolicyLoader.create( serverConfig, NullLogProvider.getInstance() ).getPolicy( TESTING );
        SslPolicy clientPolicy = SslPolicyLoader.create( clientConfig, NullLogProvider.getInstance() ).getPolicy( TESTING );
        SecureServer secureServer = new SecureServer( serverPolicy );
        secureServer.start();
        SecureClient secureClient = new SecureClient( clientPolicy );

        // then
        PemSslPolicyLoaderIT.clientCannotCommunicateWithServer( secureClient, secureServer, "signature check failed" );    }

    @Test
    public void shouldConnectWithTrustAllAndNoTrustedCerts() throws Throwable
    {
        // given server
        Config serverConfig = aConfig( serverDir, "localhost" ).set( SSL_POLICY_CONFIG.trust_all, true ).build();

        // and client
        Config clientConfig = aConfig( clientDir, "localhost" ).set( SSL_POLICY_CONFIG.trust_all, true ).build();

        // and setup
        SslPolicy serverPolicy = SslPolicyLoader.create( serverConfig, NullLogProvider.getInstance() ).getPolicy( TESTING );
        SslPolicy clientPolicy = SslPolicyLoader.create( clientConfig, NullLogProvider.getInstance() ).getPolicy( TESTING );
        SecureServer secureServer = new SecureServer( serverPolicy );
        secureServer.start();
        SecureClient secureClient = new SecureClient( clientPolicy );

        // then
        clientCanCommunicateWithServer( secureClient, secureServer );
    }

    @Test
    public void shouldNotConnectIfHostnameValidationRequestedAndHostsDoNotMatch() throws Throwable
    {
        // given server
        Config serverConfig = aConfig( serverDir, "something" ).build();

        // and client
        Config clientConfig = aConfig( clientDir, "somewhere" ).build();

        // and trust
        copyServerCertToClientTrustedCertEntry( clientDir, serverDir, KEYSTORE, KEYSTORE_PASS );

        // and setup
        SslPolicy serverPolicy = SslPolicyLoader.create( serverConfig, NullLogProvider.getInstance() ).getPolicy( TESTING );
        SslPolicy clientPolicy = SslPolicyLoader.create( clientConfig, NullLogProvider.getInstance() ).getPolicy( TESTING );
        SecureServer secureServer = new SecureServer( serverPolicy );
        secureServer.start();
        SecureClient secureClient = new SecureClient( clientPolicy );

        // then
        clientCannotCommunicateWithServer( secureClient, secureServer, "No name matching localhost found" );
    }

    @Test
    public void shouldConnectIfHostnameValidationOffAndHostsDoNotMatch() throws Throwable
    {
        // given server
        Config serverConfig = aConfig( serverDir, "something" ).set( SSL_POLICY_CONFIG.verify_hostname, false ).build();

        // and client
        Config clientConfig = aConfig( clientDir, "somewhere" ).set( SSL_POLICY_CONFIG.verify_hostname, false ).build();

        // and trust
        copyServerCertToClientTrustedCertEntry( clientDir, serverDir, KEYSTORE, KEYSTORE_PASS );

        // and setup
        SslPolicy serverPolicy = SslPolicyLoader.create( serverConfig, NullLogProvider.getInstance() ).getPolicy( TESTING );
        SslPolicy clientPolicy = SslPolicyLoader.create( clientConfig, NullLogProvider.getInstance() ).getPolicy( TESTING );
        SecureServer secureServer = new SecureServer( serverPolicy );
        secureServer.start();
        SecureClient secureClient = new SecureClient( clientPolicy );

        // then
        clientCanCommunicateWithServer( secureClient, secureServer );
    }

    @Test
    public void shouldNotConnectWithClientAuthIfClientDoesNotTrustServer() throws Throwable
    {
        // given server
        Config serverConfig = aConfig( serverDir, "localhost" ).set( SSL_POLICY_CONFIG.client_auth, ClientAuth.REQUIRE ).build();

        // and client
        Config clientConfig = aConfig( clientDir, "localhost" ).build();

        // and trust
        copyServerCertToClientTrustedCertEntry( clientDir, serverDir, KEYSTORE, KEYSTORE_PASS );

        // and setup
        SslPolicy serverPolicy = SslPolicyLoader.create( serverConfig, NullLogProvider.getInstance() ).getPolicy( TESTING );
        SslPolicy clientPolicy = SslPolicyLoader.create( clientConfig, NullLogProvider.getInstance() ).getPolicy( TESTING );
        SecureServer secureServer = new SecureServer( serverPolicy );
        secureServer.start();
        SecureClient secureClient = new SecureClient( clientPolicy );

        // then
        clientCannotCommunicateWithServer( secureClient, secureServer, "Received fatal alert: certificate_unknown" );
    }

    @Test
    public void shouldConnectWithClientAuthAndMutuallyTrustedCerts() throws Throwable
    {
        // given server
        Config serverConfig = aConfig( serverDir, "localhost" ).set( SSL_POLICY_CONFIG.client_auth, ClientAuth.REQUIRE ).build();

        // and client
        Config clientConfig = aConfig( clientDir, "localhost" ).build();

        // and trust
        copyServerCertToClientTrustedCertEntry( clientDir, serverDir, KEYSTORE, KEYSTORE_PASS );
        copyServerCertToClientTrustedCertEntry( serverDir, clientDir, KEYSTORE, KEYSTORE_PASS );

        // and setup
        SslPolicy serverPolicy = SslPolicyLoader.create( serverConfig, NullLogProvider.getInstance() ).getPolicy( TESTING );
        SslPolicy clientPolicy = SslPolicyLoader.create( clientConfig, NullLogProvider.getInstance() ).getPolicy( TESTING );
        SecureServer secureServer = new SecureServer( serverPolicy );
        secureServer.start();
        SecureClient secureClient = new SecureClient( clientPolicy );

        clientCanCommunicateWithServer( secureClient, secureServer );
    }

    @Test
    public void shouldNotCommunicateIfCiphersDoNotMatch() throws Throwable
    {
        // given server
        Config serverConfig = aConfig( serverDir, "localhost" ).set( SSL_POLICY_CONFIG.ciphers, List.of( "TLS_DHE_RSA_WITH_AES_128_CBC_SHA" ) ).build();

        // and client
        Config clientConfig = aConfig( clientDir, "localhost" ).set( SSL_POLICY_CONFIG.ciphers, List.of( "TLS_RSA_WITH_AES_128_CBC_SHA" ) ).build();

        // and trust
        copyServerCertToClientTrustedCertEntry( clientDir, serverDir, KEYSTORE, KEYSTORE_PASS );

        // and setup
        SslPolicy serverPolicy = SslPolicyLoader.create( serverConfig, NullLogProvider.getInstance() ).getPolicy( TESTING );
        SslPolicy clientPolicy = SslPolicyLoader.create( clientConfig, NullLogProvider.getInstance() ).getPolicy( TESTING );
        SecureServer secureServer = new SecureServer( serverPolicy );
        secureServer.start();
        SecureClient secureClient = new SecureClient( clientPolicy );

        // then
        clientCannotCommunicateWithServer( secureClient, secureServer, "Received fatal alert: handshake_failure" );
    }

    @Test
    public void shouldNotCommunicateIfTlsVersionsDoNotMatch() throws Throwable
    {
        // given server
        Config serverConfig = aConfig( serverDir, "localhost" ).set( SSL_POLICY_CONFIG.tls_versions, List.of( TLSV1_2 ) ).build();

        // and client
        Config clientConfig = aConfig( clientDir, "localhost" ).set( SSL_POLICY_CONFIG.tls_versions, List.of( TLSV1_1 ) ).build();

        // and trust
        copyServerCertToClientTrustedCertEntry( clientDir, serverDir, KEYSTORE, KEYSTORE_PASS );

        // and setup
        SslPolicy serverPolicy = SslPolicyLoader.create( serverConfig, NullLogProvider.getInstance() ).getPolicy( TESTING );
        SslPolicy clientPolicy = SslPolicyLoader.create( clientConfig, NullLogProvider.getInstance() ).getPolicy( TESTING );
        SecureServer secureServer = new SecureServer( serverPolicy );
        secureServer.start();
        SecureClient secureClient = new SecureClient( clientPolicy );

        // then
        clientCannotCommunicateWithServer( secureClient, secureServer, "Received fatal alert" );
    }

    private Config.Builder aConfig( File baseDirectory, String hostName ) throws IOException, InterruptedException
    {
        exec( baseDirectory, getKeyToolPath(), "-genkey", "-alias", "test", "-keystore", KEYSTORE, "-storetype", format, "-storepass", KEYSTORE_PASS,
                "-keypass", keyPass, "-keyalg", "RSA", "-dname", String.format( "cn=%s, ou=test, o=neo4j, l=london, st=european union, c=eu", hostName ) );

        File revoked = new File( baseDirectory, "revoked" );
        revoked.mkdirs();

        return Config.newBuilder()
                .set( GraphDatabaseSettings.neo4j_home, baseDirectory.toPath().toAbsolutePath() )
                .set( SSL_POLICY_CONFIG.base_directory, baseDirectory.toPath() )
                .set( SSL_POLICY_CONFIG.keystore, baseDirectory.toPath().resolve( KEYSTORE ) )
                .set( SSL_POLICY_CONFIG.keystore_pass, new SecureString( KEYSTORE_PASS ) )
                .set( SSL_POLICY_CONFIG.entry_pass, new SecureString( keyPass ) )
                .set( SSL_POLICY_CONFIG.entry_alias, KEYPAIR_ALIAS )

                .set( SSL_POLICY_CONFIG.tls_versions, List.of( TLSV1_2 ) )

                .set( SSL_POLICY_CONFIG.client_auth, ClientAuth.NONE )

                // Even if we trust all, certs should be rejected if don't match Common Name (CA) or Subject Alternative Name
                .set( SSL_POLICY_CONFIG.trust_all, false )
                .set( SSL_POLICY_CONFIG.verify_hostname, true );
    }

    private static String getKeyToolPath()
    {
        return Path.of( SystemUtils.getJavaHome().getAbsolutePath(), "bin", "keytool" ).toString();
    }

    private void signServerCertWithCAAndCopyCACertToClientTrustedCertEntry( File serverBaseDir, File clientBaseDir ) throws IOException, InterruptedException
    {
        String certReq = "certreq";
        String signed = "signed";

        String ca = "ca";

        // Create CA
        String keyToolPath = getKeyToolPath();
        exec( serverBaseDir, keyToolPath, "-genkey", "-alias", CA_ALIAS, "-keystore", KEYSTORE, "-storetype", format, "-storepass", KEYSTORE_PASS,
                "-keypass", keyPass, "-keyalg", "RSA", "-dname", "cn=ca, ou=test, o=neo4j, l=london, st=european union, c=eu", "-ext", "bc=ca:true" );

        // Generate cert req for server
        exec( serverBaseDir, keyToolPath, "-certreq", "-alias", KEYPAIR_ALIAS, "-keystore", KEYSTORE, "-storepass", KEYSTORE_PASS,
                "-file", certReq, "-keypass", keyPass,  "-keyalg", "rsa" );

        // Sign cert req
        exec( serverBaseDir, keyToolPath, "-gencert", "-alias", CA_ALIAS, "-keystore", KEYSTORE, "-storepass", KEYSTORE_PASS,
                "-infile", certReq, "-outfile", signed, "-keypass", keyPass, "-ext", "san=dns:localhost" );

        // Import cert req
        exec( serverBaseDir, keyToolPath, "-importcert", "-alias", KEYPAIR_ALIAS, "-keystore", KEYSTORE, "-file", signed, "-deststoretype", format,
                "-storepass", KEYSTORE_PASS, "-keypass", keyPass, "-noprompt" );

        // Export CA cert
        exec( serverBaseDir, keyToolPath, "-exportcert", "-alias", CA_ALIAS, "-file", ca, "-keystore", KEYSTORE, "-storepass", KEYSTORE_PASS,
                "-keypass", keyPass );

        // Trust CA cert on client
        Path exportedFile = serverBaseDir.toPath().resolve( ca );
        exec( clientBaseDir, keyToolPath, "-importcert", "-alias", TRUSTED_ALIAS, "-keystore", KEYSTORE, "-file", exportedFile.toString(),
                "-deststoretype", format, "-storepass", KEYSTORE_PASS, "-noprompt" );

        Files.delete( exportedFile );
    }

    private void copyServerCertToClientTrustedCertEntry( File clientBaseDir, File serverBaseDir, String trustStore, String storePass )
            throws IOException, InterruptedException
    {
        String filename = "cer.cer";
        String keyToolPath = getKeyToolPath();
        exec( serverBaseDir, keyToolPath, "-exportcert", "-alias", KEYPAIR_ALIAS, "-file", filename, "-keystore", KEYSTORE, "-storepass", KEYSTORE_PASS,
                        "-keypass", keyPass );

        Path exportedFile = serverBaseDir.toPath().resolve( filename );

        exec( clientBaseDir, keyToolPath, "-importcert", "-alias", TRUSTED_ALIAS, "-keystore", trustStore, "-file", exportedFile.toString(),
                "-deststoretype", format, "-storepass", storePass, "-noprompt" );

        Files.delete( exportedFile );
    }

    private static void exec( File directory, String... commands ) throws IOException, InterruptedException
    {
        new ProcessBuilder()
                .command( commands )
                .directory( directory )
                .redirectOutput( ProcessBuilder.Redirect.appendTo( directory.toPath().resolve( "output" ).toFile() ) )
                .redirectError( ProcessBuilder.Redirect.appendTo( directory.toPath().resolve( "error" ).toFile() ) )
                .start()
                .waitFor( 30, TimeUnit.SECONDS );
    }

    private void assumeFormatSupported()
    {
        try
        {
            KeyStore.getInstance( format );
        }
        catch ( KeyStoreException e )
        {
            Assume.assumeNoException( e );
        }
    }
}
