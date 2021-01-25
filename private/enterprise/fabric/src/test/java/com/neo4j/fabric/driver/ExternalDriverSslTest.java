/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.fabric.driver;

import com.neo4j.configuration.FabricEnterpriseConfig;

import java.util.Map;

import org.neo4j.configuration.Config;
import org.neo4j.ssl.config.SslPolicyLoader;

class ExternalDriverSslTest extends AbstractDriverSslTest
{
    @Override
    Map<String,String> getSettingValues( String serverUri,  SslDir sslResource, String policyBaseDir, boolean verifyHostname  )
    {
        return Map.of(
                "fabric.database.name", "mega",
                "fabric.graph.0.uri", serverUri,
                "dbms.ssl.policy.fabric.enabled", "true",
                "dbms.ssl.policy.fabric.base_directory", policyBaseDir,
                "dbms.ssl.policy.fabric.private_key", sslResource.getPrivateKey().toString(),
                "dbms.ssl.policy.fabric.public_certificate", sslResource.getPublicCertificate().toString(),
                "dbms.ssl.policy.fabric.trusted_dir", sslResource.getTrustedDirectory().toString(),
                "dbms.ssl.policy.fabric.revoked_dir", sslResource.getRevokedDirectory().toString(),
                "dbms.ssl.policy.fabric.verify_hostname", Boolean.toString( verifyHostname ) );
    }

    @Override
    DriverConfigFactory getConfigFactory( FabricEnterpriseConfig fabricConfig, Config serverConfig, SslPolicyLoader sslPolicyLoader )
    {
        return new ExternalDriverConfigFactory( fabricConfig, serverConfig, sslPolicyLoader );
    }

    @Override
    boolean requiresPrivateKey()
    {
        return false;
    }
}
