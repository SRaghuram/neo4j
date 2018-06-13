/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.ssl;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import org.bouncycastle.operator.OperatorCreationException;
import org.hamcrest.core.IsCollectionContaining;
import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.Objects;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.neo4j.com.storecopy.FileMoveAction;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.configuration.ssl.SslPolicyConfig;
import org.neo4j.kernel.configuration.ssl.SslPolicyLoader;
import org.neo4j.kernel.configuration.ssl.TrustManagerFactoryProvider;
import org.neo4j.logging.FormattedLogProvider;
import org.neo4j.logging.Level;
import org.neo4j.logging.LogProvider;
import org.neo4j.test.rule.TestDirectory;

import static java.util.concurrent.TimeUnit.MINUTES;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class SslPolicyLoaderIT
{
    @Rule
    public TestDirectory testDirectory = TestDirectory.testDirectory();

    private static final PkiUtils PKI_UTILS = new PkiUtils();
    private static final LogProvider LOG_PROVIDER = FormattedLogProvider.withDefaultLogLevel( Level.DEBUG ).toOutputStream( System.out );
    private static final String POLICY_NAME = "fakePolicy";

    @Test
    public void certificatesWithInvalidCommonNameAreRejected() throws GeneralSecurityException, IOException, OperatorCreationException, InterruptedException
    {
        // given server has a certificate that matches an invalid hostname
        Config serverConfig = aConfig( "invalid-not-localhost" );

        // and client has any certificate (valid), since hostname validation is done from the client side
        Config clientConfig = aConfig( "localhost" );

        trust( serverConfig, clientConfig );
        trust( clientConfig, serverConfig );

        // and setup
        SslPolicy serverPolicy = SslPolicyLoader.create( serverConfig, new TrustManagerFactoryProvider(), LOG_PROVIDER ).getPolicy( POLICY_NAME );
        SslPolicy clientPolicy = SslPolicyLoader.create( clientConfig, new TrustManagerFactoryProvider(), LOG_PROVIDER ).getPolicy( POLICY_NAME );
        SecureServer secureServer = new SecureServer( serverPolicy.nettyServerContext(), true, LOG_PROVIDER );
        secureServer.start();
        int port = secureServer.port();
        SecureClient secureClient = new SecureClient( clientPolicy.nettyClientContext(), true, LOG_PROVIDER );

        // when client connects to server with a non-matching hostname
        try
        {
            secureClient.connect( port );

            // then handshake complete with exception describing hostname mismatch
            assertTrue( secureClient.sslHandshakeFuture().await( 1, MINUTES ) );
            String expectedMessage = "No subject alternative DNS name matching localhost found.";
            assertThat( causes( secureClient.sslHandshakeFuture().cause() ).map( Throwable::getMessage ).collect( Collectors.toList() ),
                    IsCollectionContaining.hasItem( expectedMessage ) );
        }
        finally
        {
            secureServer.stop();
        }
    }

    @Test
    public void normalBehaviourIfServerCertificateMatchesClientExpectation()
            throws GeneralSecurityException, IOException, OperatorCreationException, InterruptedException
    {
        // given server has valid hostname
        Config serverConfig = aConfig( "localhost" );

        // and client has invalid hostname (which is irrelevant for hostname verification)
        Config clientConfig = aConfig( "invalid-localhost" );

        trust( serverConfig, clientConfig );
        trust( clientConfig, serverConfig );

        // and setup
        SslPolicy serverPolicy = SslPolicyLoader.create( serverConfig, new TrustManagerFactoryProvider(), LOG_PROVIDER ).getPolicy( POLICY_NAME );
        SslPolicy clientPolicy = SslPolicyLoader.create( clientConfig, new TrustManagerFactoryProvider(), LOG_PROVIDER ).getPolicy( POLICY_NAME );
        SecureServer secureServer = new SecureServer( serverPolicy.nettyServerContext(), true );
        secureServer.start();
        SecureClient secureClient = new SecureClient( clientPolicy.nettyClientContext(), true );

        // then
        clientCanCommunicateWithServer( secureClient, secureServer );
    }

    @Test
    public void legacyPolicyDoesNotHaveHostnameVerification() throws GeneralSecurityException, IOException, OperatorCreationException, InterruptedException
    {
        // given server has an invalid hostname
        Config serverConfig = aConfig( "invalid-localhost" );

        // and client has invalid hostname (which is irrelevant for hostname verification)
        Config clientConfig = aConfig( "invalid-localhost" );

        trust( serverConfig, clientConfig );
        trust( clientConfig, serverConfig );

        // and setup
        SslPolicy serverPolicy = SslPolicyLoader.create( serverConfig, new TrustManagerFactoryProvider(), LOG_PROVIDER ).getPolicy( POLICY_NAME );
        SslPolicy clientPolicy = SslPolicyLoader.create( clientConfig, new TrustManagerFactoryProvider(), LOG_PROVIDER ).getPolicy( "legacy" );
        SecureServer secureServer = new SecureServer( serverPolicy.nettyServerContext(), true );
        secureServer.start();
        SecureClient secureClient = new SecureClient( clientPolicy.nettyClientContext(), true );

        // then
        clientCanCommunicateWithServer( secureClient, secureServer );
    }

    private void clientCanCommunicateWithServer( SecureClient secureClient, SecureServer secureServer ) throws InterruptedException
    {
        int port = secureServer.port();
        try
        {
            secureClient.connect( port );
            ByteBuf request = ByteBufAllocator.DEFAULT.buffer().writeBytes( new byte[]{1, 2, 3, 4} );
            secureClient.channel().writeAndFlush( request );

            ByteBuf expected = ByteBufAllocator.DEFAULT.buffer().writeBytes( SecureServer.RESPONSE );
            assertTrue( secureClient.sslHandshakeFuture().await( 1, MINUTES ) );
            secureClient.assertResponse( expected );
        }
        finally
        {
            secureServer.stop();
        }
    }

    private Config aConfig( String hostname ) throws GeneralSecurityException, IOException, OperatorCreationException
    {
        SslPolicyConfig sslPolicyConfig = new SslPolicyConfig( POLICY_NAME );
        String random = UUID.randomUUID().toString();
        File baseDirectory = testDirectory.directory( "base_directory_" + random );
        File validCertificatePath = new File( baseDirectory, "certificate.crt" );
        File validPrivateKeyPath = new File( baseDirectory, "private.pem" );
        File revoked = new File( baseDirectory, "revoked" );
        File trusted = new File( baseDirectory, "trusted" );
        trusted.mkdirs();
        revoked.mkdirs();
        PKI_UTILS.createSelfSignedCertificate( validCertificatePath, validPrivateKeyPath, hostname ); // Sets Subject Alternative Name(s) to hostname
        return Config.builder()
                .withSetting( sslPolicyConfig.base_directory, baseDirectory.toString() )
                .withSetting( sslPolicyConfig.trusted_dir, trusted.toString() )
                .withSetting( sslPolicyConfig.revoked_dir, revoked.toString() )
                .withSetting( sslPolicyConfig.private_key, validPrivateKeyPath.toString() )
                .withSetting( sslPolicyConfig.public_certificate, validCertificatePath.toString() )

                .withSetting( sslPolicyConfig.tls_versions, "TLSv1.2" )
                .withSetting( sslPolicyConfig.ciphers, "TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA" )

                .withSetting( sslPolicyConfig.client_auth, "none" )
                .withSetting( sslPolicyConfig.allow_key_generation, "false" )

                // Even if we trust all, certs should be rejected if don't match Common Name (CA) or Subject Alternative Name
                .withSetting( sslPolicyConfig.trust_all, "false" )
                .withSetting( sslPolicyConfig.verify_hostname, "true" )
                .build();
    }

    private void trust( Config target, Config subject ) throws IOException
    {
        SslPolicyConfig sslPolicyConfig = new SslPolicyConfig( POLICY_NAME );
        File trustedDirectory = target.get( sslPolicyConfig.trusted_dir );
        File certificate = subject.get( sslPolicyConfig.public_certificate );
        FileMoveAction.copyViaFileSystem( certificate, certificate.getParentFile() ).move( trustedDirectory );
    }

    private Stream<Throwable> causes( Throwable throwable )
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
