/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.ssl;

import org.junit.Assume;
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
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.ssl.ClientAuth;
import org.neo4j.configuration.ssl.KeyStoreSslPolicyConfig;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.ssl.config.SslPolicyLoader;
import org.neo4j.test.rule.TestDirectory;

import static org.neo4j.ssl.PemSslPolicyLoaderIT.clientCanCommunicateWithServer;
import static org.neo4j.ssl.PemSslPolicyLoaderIT.clientCannotCommunicateWithServer;

@RunWith( Parameterized.class )
public class KeyStoreSslPolicyLoaderIT
{
    @Rule
    public TestDirectory testDirectory = TestDirectory.testDirectory();

    private static final String TLSV1_1 = "TLSv1.1";
    private static final String TLSV1_2 = "TLSv1.2";
    private static final String TLSV1_3 = "TLSv1.3";
    private static final String TRUSTED_ALIAS = "trusted";
    private static final String POLICY_NAME = "fakePolicy";
    private static final KeyStoreSslPolicyConfig SSL_POLICY_CONFIG = new KeyStoreSslPolicyConfig( POLICY_NAME );
    private static final String KEYPAIR_ALIAS = "test";
    private static final String KEYSTORE = "keystore";
    private static final String STORE_PASS = "foobar";
    private static final String CA_ALIAS = "CA";

    private static final String JKS = "jks";
    private static final String PKCS12 = "pkcs12";

    private final String keyPass;
    private final String format;

    @Parameterized.Parameters( name = "{0}" )
    public static Collection<String> data()
    {
        return Arrays.asList( JKS, PKCS12 );
    }

    public KeyStoreSslPolicyLoaderIT( String format )
    {
        this.format = format;
        this.keyPass = format.equals( PKCS12 ) ? STORE_PASS : "barquux";
    }

    @Test
    public void shouldConnectWithServerTrustedByClient() throws Throwable
    {
        assumeFormatSupported();

        // given server
        File serverDir = testDirectory.directory( "base_directory_" + UUID.randomUUID() );
        Config serverConfig = aConfig( serverDir, "localhost" ).build();

        // and client
        File clientDir = testDirectory.directory( "base_directory_" + UUID.randomUUID() );
        Config clientConfig = aConfig( clientDir, "localhost" ).build();

        // and trust
        copyServerCertToClientTrustedCertEntry( clientDir, serverDir );

        // and setup
        SslPolicy serverPolicy = SslPolicyLoader.create( serverConfig, NullLogProvider.getInstance() ).getPolicy( POLICY_NAME );
        SslPolicy clientPolicy = SslPolicyLoader.create( clientConfig, NullLogProvider.getInstance() ).getPolicy( POLICY_NAME );
        SecureServer secureServer = new SecureServer( serverPolicy );
        secureServer.start();
        SecureClient secureClient = new SecureClient( clientPolicy );

        // then
        clientCanCommunicateWithServer( secureClient, secureServer );
    }

    @Test
    public void shouldConnectWithCATrustedCert() throws Throwable
    {
        assumeFormatSupported();

        // given server
        File serverDir = testDirectory.directory( "base_directory_" + UUID.randomUUID() );
        Config serverConfig = aConfig( serverDir, "localhost" ).build();

        // and client
        File clientDir = testDirectory.directory( "base_directory_" + UUID.randomUUID() );
        Config clientConfig = aConfig( clientDir, "localhost" ).build();

        // and trust
        signServerCertWithCAAndCopyCACertToClientTrustedCertEntry( serverDir, clientDir );

        // and setup
        SslPolicy serverPolicy = SslPolicyLoader.create( serverConfig, NullLogProvider.getInstance() ).getPolicy( POLICY_NAME );
        SslPolicy clientPolicy = SslPolicyLoader.create( clientConfig, NullLogProvider.getInstance() ).getPolicy( POLICY_NAME );
        SecureServer secureServer = new SecureServer( serverPolicy );
        secureServer.start();
        SecureClient secureClient = new SecureClient( clientPolicy );

        // then
        clientCanCommunicateWithServer( secureClient, secureServer );
    }

    @Test
    public void shouldNotConnectWithoutTrustedCerts() throws Throwable
    {
        assumeFormatSupported();

        // given server
        File serverDir = testDirectory.directory( "base_directory_" + UUID.randomUUID() );
        Config serverConfig = aConfig( serverDir, "localhost" ).build();

        // and client
        File clientDir = testDirectory.directory( "base_directory_" + UUID.randomUUID() );
        Config clientConfig = aConfig( clientDir, "localhost" ).build();

        // and setup
        SslPolicy serverPolicy = SslPolicyLoader.create( serverConfig, NullLogProvider.getInstance() ).getPolicy( POLICY_NAME );
        SslPolicy clientPolicy = SslPolicyLoader.create( clientConfig, NullLogProvider.getInstance() ).getPolicy( POLICY_NAME );
        SecureServer secureServer = new SecureServer( serverPolicy );
        secureServer.start();
        SecureClient secureClient = new SecureClient( clientPolicy );

        // then
        PemSslPolicyLoaderIT.clientCannotCommunicateWithServer( secureClient, secureServer, "signature check failed" );    }

    @Test
    public void shouldConnectWithTrustAllAndNoTrustedCerts() throws Throwable
    {
        assumeFormatSupported();

        // given server
        File serverDir = testDirectory.directory( "base_directory_" + UUID.randomUUID() );
        Config serverConfig = aConfig( serverDir, "localhost" ).withSetting( SSL_POLICY_CONFIG.trust_all, "true" ).build();

        // and client
        File clientDir = testDirectory.directory( "base_directory_" + UUID.randomUUID() );
        Config clientConfig = aConfig( clientDir, "localhost" ).withSetting( SSL_POLICY_CONFIG.trust_all, "true" ).build();

        // and setup
        SslPolicy serverPolicy = SslPolicyLoader.create( serverConfig, NullLogProvider.getInstance() ).getPolicy( POLICY_NAME );
        SslPolicy clientPolicy = SslPolicyLoader.create( clientConfig, NullLogProvider.getInstance() ).getPolicy( POLICY_NAME );
        SecureServer secureServer = new SecureServer( serverPolicy );
        secureServer.start();
        SecureClient secureClient = new SecureClient( clientPolicy );

        // then
        clientCanCommunicateWithServer( secureClient, secureServer );
    }

    @Test
    public void shouldNotConnectIfHostnameValidationRequestedAndHostsDoNotMatch() throws Throwable
    {
        assumeFormatSupported();

        // given server
        File serverDir = testDirectory.directory( "base_directory_" + UUID.randomUUID() );
        Config serverConfig = aConfig( serverDir, "something" ).build();

        // and client
        File clientDir = testDirectory.directory( "base_directory_" + UUID.randomUUID() );
        Config clientConfig = aConfig( clientDir, "somewhere" ).build();

        // and trust
        copyServerCertToClientTrustedCertEntry( clientDir, serverDir );

        // and setup
        SslPolicy serverPolicy = SslPolicyLoader.create( serverConfig, NullLogProvider.getInstance() ).getPolicy( POLICY_NAME );
        SslPolicy clientPolicy = SslPolicyLoader.create( clientConfig, NullLogProvider.getInstance() ).getPolicy( POLICY_NAME );
        SecureServer secureServer = new SecureServer( serverPolicy );
        secureServer.start();
        SecureClient secureClient = new SecureClient( clientPolicy );

        // then
        clientCannotCommunicateWithServer( secureClient, secureServer, "No name matching localhost found" );
    }

    @Test
    public void shouldConnectIfHostnameValidationOffAndHostsDoNotMatch() throws Throwable
    {
        assumeFormatSupported();

        // given server
        File serverDir = testDirectory.directory( "base_directory_" + UUID.randomUUID() );
        Config serverConfig = aConfig( serverDir, "something" ).withSetting( SSL_POLICY_CONFIG.verify_hostname, "false" ).build();

        // and client
        File clientDir = testDirectory.directory( "base_directory_" + UUID.randomUUID() );
        Config clientConfig = aConfig( clientDir, "somewhere" ).withSetting( SSL_POLICY_CONFIG.verify_hostname, "false" ).build();

        // and trust
        copyServerCertToClientTrustedCertEntry( clientDir, serverDir );

        // and setup
        SslPolicy serverPolicy = SslPolicyLoader.create( serverConfig, NullLogProvider.getInstance() ).getPolicy( POLICY_NAME );
        SslPolicy clientPolicy = SslPolicyLoader.create( clientConfig, NullLogProvider.getInstance() ).getPolicy( POLICY_NAME );
        SecureServer secureServer = new SecureServer( serverPolicy );
        secureServer.start();
        SecureClient secureClient = new SecureClient( clientPolicy );

        // then
        clientCanCommunicateWithServer( secureClient, secureServer );
    }

    @Test
    public void shouldNotConnectWithClientAuthIfClientDoesNotTrustServer() throws Throwable
    {
        assumeFormatSupported();

        // given server
        File serverDir = testDirectory.directory( "base_directory_" + UUID.randomUUID() );
        Config serverConfig = aConfig( serverDir, "localhost" ).withSetting( SSL_POLICY_CONFIG.client_auth, ClientAuth.REQUIRE.name() ).build();

        // and client
        File clientDir = testDirectory.directory( "base_directory_" + UUID.randomUUID() );
        Config clientConfig = aConfig( clientDir, "localhost" ).build();

        // and trust
        copyServerCertToClientTrustedCertEntry( clientDir, serverDir );

        // and setup
        SslPolicy serverPolicy = SslPolicyLoader.create( serverConfig, NullLogProvider.getInstance() ).getPolicy( POLICY_NAME );
        SslPolicy clientPolicy = SslPolicyLoader.create( clientConfig, NullLogProvider.getInstance() ).getPolicy( POLICY_NAME );
        SecureServer secureServer = new SecureServer( serverPolicy );
        secureServer.start();
        SecureClient secureClient = new SecureClient( clientPolicy );

        // then
        clientCannotCommunicateWithServer( secureClient, secureServer, "Received fatal alert: certificate_unknown" );
    }

    @Test
    public void shouldConnectWithClientAuthAndMutuallyTrustedCerts() throws Throwable
    {
        assumeFormatSupported();

        // given server
        File serverDir = testDirectory.directory( "base_directory_" + UUID.randomUUID() );
        Config serverConfig = aConfig( serverDir, "localhost" ).withSetting( SSL_POLICY_CONFIG.client_auth, ClientAuth.REQUIRE.name() ).build();

        // and client
        File clientDir = testDirectory.directory( "base_directory_" + UUID.randomUUID() );
        Config clientConfig = aConfig( clientDir, "localhost" ).build();

        // and trust
        copyServerCertToClientTrustedCertEntry( clientDir, serverDir );
        copyServerCertToClientTrustedCertEntry( serverDir, clientDir );

        // and setup
        SslPolicy serverPolicy = SslPolicyLoader.create( serverConfig, NullLogProvider.getInstance() ).getPolicy( POLICY_NAME );
        SslPolicy clientPolicy = SslPolicyLoader.create( clientConfig, NullLogProvider.getInstance() ).getPolicy( POLICY_NAME );
        SecureServer secureServer = new SecureServer( serverPolicy );
        secureServer.start();
        SecureClient secureClient = new SecureClient( clientPolicy );

        clientCanCommunicateWithServer( secureClient, secureServer );
    }

    @Test
    public void shouldNotCommunicateIfCiphersDoNotMatch() throws Throwable
    {
        assumeFormatSupported();

        // given server
        File serverDir = testDirectory.directory( "base_directory_" + UUID.randomUUID() );
        Config serverConfig = aConfig( serverDir, "localhost" ).withSetting( SSL_POLICY_CONFIG.ciphers, "TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA" ).build();

        // and client
        File clientDir = testDirectory.directory( "base_directory_" + UUID.randomUUID() );
        Config clientConfig = aConfig( clientDir, "localhost" ).withSetting( SSL_POLICY_CONFIG.ciphers, "TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA" ).build();

        // and trust
        copyServerCertToClientTrustedCertEntry( clientDir, serverDir );

        // and setup
        SslPolicy serverPolicy = SslPolicyLoader.create( serverConfig, NullLogProvider.getInstance() ).getPolicy( POLICY_NAME );
        SslPolicy clientPolicy = SslPolicyLoader.create( clientConfig, NullLogProvider.getInstance() ).getPolicy( POLICY_NAME );
        SecureServer secureServer = new SecureServer( serverPolicy );
        secureServer.start();
        SecureClient secureClient = new SecureClient( clientPolicy );

        // then
        clientCannotCommunicateWithServer( secureClient, secureServer, "Received fatal alert: handshake_failure" );
    }

    @Test
    public void shouldNotCommunicateIfTlsVersionsDoNotMatch() throws Throwable
    {
        assumeFormatSupported();

        // given server
        File serverDir = testDirectory.directory( "base_directory_" + UUID.randomUUID() );
        Config serverConfig = aConfig( serverDir, "localhost" ).withSetting( SSL_POLICY_CONFIG.tls_versions, TLSV1_2 ).build();

        // and client
        File clientDir = testDirectory.directory( "base_directory_" + UUID.randomUUID() );

        Config clientConfig = aConfig( clientDir, "localhost" ).withSetting( SSL_POLICY_CONFIG.tls_versions, TLSV1_1 ).build();

        // and trust
        copyServerCertToClientTrustedCertEntry( clientDir, serverDir );

        // and setup
        SslPolicy serverPolicy = SslPolicyLoader.create( serverConfig, NullLogProvider.getInstance() ).getPolicy( POLICY_NAME );
        SslPolicy clientPolicy = SslPolicyLoader.create( clientConfig, NullLogProvider.getInstance() ).getPolicy( POLICY_NAME );
        SecureServer secureServer = new SecureServer( serverPolicy );
        secureServer.start();
        SecureClient secureClient = new SecureClient( clientPolicy );

        // then
        clientCannotCommunicateWithServer( secureClient, secureServer, "Received fatal alert" );
    }

    private Config.Builder aConfig( File baseDirectory, String hostName ) throws IOException, InterruptedException
    {

        exec( baseDirectory,"keytool", "-genkey", "-alias", "test", "-keystore", KEYSTORE, "-storetype", format, "-storepass", STORE_PASS,
                "-keypass", keyPass, "-keyalg", "RSA", "-dname", String.format( "cn=%s, ou=test, o=neo4j, l=london, st=european union, c=eu", hostName ) );

        File revoked = new File( baseDirectory, "revoked" );
        revoked.mkdirs();

        return Config.builder()
                .withSetting( SSL_POLICY_CONFIG.format, format )

                .withSetting( SSL_POLICY_CONFIG.base_directory, baseDirectory.toString() )

                .withSetting( SSL_POLICY_CONFIG.keystore, baseDirectory.toPath().resolve( KEYSTORE ).toString() )
                .withSetting( SSL_POLICY_CONFIG.keystore_pass, STORE_PASS )
                .withSetting( SSL_POLICY_CONFIG.entry_pass, keyPass )
                .withSetting( SSL_POLICY_CONFIG.entry_alias, KEYPAIR_ALIAS )

                .withSetting( SSL_POLICY_CONFIG.tls_versions, TLSV1_2 )

                .withSetting( SSL_POLICY_CONFIG.client_auth, ClientAuth.NONE.name() )

                // Even if we trust all, certs should be rejected if don't match Common Name (CA) or Subject Alternative Name
                .withSetting( SSL_POLICY_CONFIG.trust_all, "false" )
                .withSetting( SSL_POLICY_CONFIG.verify_hostname, "true" );
    }

    private void signServerCertWithCAAndCopyCACertToClientTrustedCertEntry( File serverBaseDir, File clientBaseDir ) throws IOException, InterruptedException
    {
        String certReq = "certreq";
        String signed = "signed";

        String ca = "ca";

        // Create CA
        exec( serverBaseDir,"keytool", "-genkey", "-alias", CA_ALIAS, "-keystore", KEYSTORE, "-storetype", format, "-storepass", STORE_PASS,
                "-keypass", keyPass, "-keyalg", "RSA", "-dname", "cn=ca, ou=test, o=neo4j, l=london, st=european union, c=eu", "-ext", "bc=ca:true" );

        // Generate cert req for server
        exec( serverBaseDir, "keytool", "-certreq", "-alias", KEYPAIR_ALIAS, "-keystore", KEYSTORE, "-storetype", format, "-storepass", STORE_PASS,
                "-file", certReq, "-keypass", keyPass,  "-keyalg", "rsa" );

        // Sign cert req
        exec( serverBaseDir,"keytool", "-gencert", "-alias", CA_ALIAS, "-keystore", KEYSTORE, "-storetype", format, "-storepass", STORE_PASS,
                "-infile", certReq, "-outfile", signed, "-keypass", keyPass, "-ext", "san=dns:localhost" );

        // Import cert req
        exec( serverBaseDir, "keytool", "-importcert", "-alias", KEYPAIR_ALIAS, "-keystore", KEYSTORE, "-file", signed,
                "-storepass", STORE_PASS, "-keypass", keyPass, "-noprompt" );

        // Export CA cert
        exec( serverBaseDir, "keytool", "-exportcert", "-alias", CA_ALIAS, "-file", ca, "-keystore", KEYSTORE, "-storepass", STORE_PASS,
                "-keypass", keyPass );

        // Trust CA cert on client
        Path exportedFile = serverBaseDir.toPath().resolve( ca );
        exec( clientBaseDir, "keytool", "-importcert", "-alias", TRUSTED_ALIAS, "-keystore", KEYSTORE, "-file", exportedFile.toString(),
                "-storepass", STORE_PASS, "-noprompt" );

        Files.delete( exportedFile );
    }

    private void copyServerCertToClientTrustedCertEntry( File clientBaseDir, File serverBaseDir ) throws IOException, InterruptedException
    {
        String filename = "cer.cer";
        exec( serverBaseDir, "keytool", "-exportcert", "-alias", KEYPAIR_ALIAS, "-file", filename, "-keystore", KEYSTORE, "-storepass", STORE_PASS,
                        "-keypass", keyPass );

        Path exportedFile = serverBaseDir.toPath().resolve( filename );

        exec( clientBaseDir, "keytool", "-importcert", "-alias", TRUSTED_ALIAS, "-keystore", KEYSTORE, "-file", exportedFile.toString(),
                "-storepass", STORE_PASS, "-noprompt" );

        Files.delete( exportedFile );
    }

    private void exec( File directory, String... commands ) throws IOException, InterruptedException
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
