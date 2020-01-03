/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
import org.neo4j.configuration.ssl.SslPolicyConfig;
import org.neo4j.configuration.ssl.SslPolicyScope;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.test.ssl.SelfSignedCertificateFactory;

public class HostnameVerificationHelper
{

    private static final SelfSignedCertificateFactory certFactory = new SelfSignedCertificateFactory();

    public static Config aConfig( String hostname, TestDirectory testDirectory, SslPolicyScope scope )
            throws GeneralSecurityException, IOException, OperatorCreationException
    {
        SslPolicyConfig policy = SslPolicyConfig.forScope( scope );
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

                .set( policy.enabled, Boolean.TRUE )
                .set( policy.base_directory, baseDirectory.toPath() )
                .set( policy.trusted_dir, trusted.toPath() )
                .set( policy.revoked_dir, revoked.toPath() )
                .set( policy.private_key, validPrivateKeyPath.toPath() )
                .set( policy.public_certificate, validCertificatePath.toPath() )

                .set( policy.tls_versions, List.of( "TLSv1.2" ) )
                .set( policy.ciphers, List.of( "TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA" ) )

                .set( policy.client_auth, ClientAuth.NONE )

                // Even if we trust all, certs should be rejected if don't match Common Name (CA) or Subject Alternative Name
                .set( policy.trust_all, false )
                .set( policy.verify_hostname, true )
                .build();
    }

    public static void trust( Config target, Config subject, SslPolicyScope scope ) throws IOException
    {
        SslPolicyConfig sslPolicyConfig = SslPolicyConfig.forScope( scope );
        Path trustedDirectory = target.get( sslPolicyConfig.trusted_dir );
        File certificate = subject.get( sslPolicyConfig.public_certificate ).toFile();
        Path trustedCertFilePath = trustedDirectory.resolve( certificate.getName() );
        Files.copy( certificate.toPath(), trustedCertFilePath );
    }
}
