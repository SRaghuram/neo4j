/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.ssl;

import org.bouncycastle.operator.OperatorCreationException;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.util.List;
import java.util.UUID;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.ssl.ClientAuth;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.configuration.ssl.PemSslPolicyConfig;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.test.ssl.SelfSignedCertificateFactory;

public class HostnameVerificationHelper
{
    public static final String POLICY_NAME = "fakePolicy";
    public static final PemSslPolicyConfig SSL_POLICY_CONFIG = PemSslPolicyConfig.group( POLICY_NAME );
    private static final SelfSignedCertificateFactory certFactory = new SelfSignedCertificateFactory();

    public static Config aConfig( String hostname, TestDirectory testDirectory ) throws GeneralSecurityException, IOException, OperatorCreationException
    {
        String random = UUID.randomUUID().toString();
        File baseDirectory = testDirectory.directory( "base_directory_" + random );
        File validCertificatePath = new File( baseDirectory, "certificate.crt" );
        File validPrivateKeyPath = new File( baseDirectory, "private.pem" );
        File revoked = new File( baseDirectory, "revoked" );
        File trusted = new File( baseDirectory, "trusted" );
        trusted.mkdirs();
        revoked.mkdirs();
        certFactory.createSelfSignedCertificate( validCertificatePath, validPrivateKeyPath, hostname ); // Sets Subject Alternative Name(s) to hostname
        return Config.newBuilder()
                .set( GraphDatabaseSettings.legacy_certificates_directory, testDirectory.directory( "certificates" ).toPath() )

                .set( SSL_POLICY_CONFIG.base_directory, baseDirectory.toPath() )
                .set( SSL_POLICY_CONFIG.trusted_dir, trusted.toPath() )
                .set( SSL_POLICY_CONFIG.revoked_dir, revoked.toPath() )
                .set( SSL_POLICY_CONFIG.private_key, validPrivateKeyPath.toPath() )
                .set( SSL_POLICY_CONFIG.public_certificate, validCertificatePath.toPath() )

                .set( SSL_POLICY_CONFIG.tls_versions, List.of( "TLSv1.2" ) )
                .set( SSL_POLICY_CONFIG.ciphers, List.of( "TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA" ) )

                .set( SSL_POLICY_CONFIG.client_auth, ClientAuth.NONE )

                // Even if we trust all, certs should be rejected if don't match Common Name (CA) or Subject Alternative Name
                .set( SSL_POLICY_CONFIG.trust_all, false )
                .set( SSL_POLICY_CONFIG.verify_hostname, true )
                .build();
    }

    public static void trust( Config target, Config subject ) throws IOException
    {
        PemSslPolicyConfig sslPolicyConfig = PemSslPolicyConfig.group( POLICY_NAME );
        Path trustedDirectory = target.get( sslPolicyConfig.trusted_dir );
        File certificate = subject.get( sslPolicyConfig.public_certificate ).toFile();
        Path trustedCertFilePath = trustedDirectory.resolve( certificate.getName() );
        Files.copy( certificate.toPath(), trustedCertFilePath );
    }
}
