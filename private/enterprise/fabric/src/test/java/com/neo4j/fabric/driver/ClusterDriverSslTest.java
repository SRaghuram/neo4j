/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.fabric.driver;

import com.neo4j.configuration.FabricEnterpriseConfig;

import java.util.Map;

import org.neo4j.configuration.Config;
import org.neo4j.ssl.config.SslPolicyLoader;

class ClusterDriverSslTest extends AbstractDriverSslTest
{
    @Override
    Map<String,String> getSettingValues( String serverUri,  SslDir sslResource, String policyBaseDir, boolean verifyHostname  )
    {
        return Map.of(
                "dbms.ssl.policy.cluster.enabled", "true",
                "dbms.ssl.policy.cluster.base_directory", policyBaseDir,
                "dbms.ssl.policy.cluster.private_key", sslResource.getPrivateKey().toString(),
                "dbms.ssl.policy.cluster.public_certificate", sslResource.getPublicCertificate().toString(),
                "dbms.ssl.policy.cluster.trusted_dir", sslResource.getTrustedDirectory().toString(),
                "dbms.ssl.policy.cluster.revoked_dir", sslResource.getRevokedDirectory().toString(),
                "dbms.ssl.policy.cluster.verify_hostname", Boolean.toString( verifyHostname ) );
    }

    @Override
    DriverConfigFactory getConfigFactory( FabricEnterpriseConfig fabricConfig, Config serverConfig, SslPolicyLoader sslPolicyLoader )
    {
        return new ClusterDriverConfigFactory( fabricConfig, serverConfig, sslPolicyLoader );
    }

    @Override
    boolean requiresPrivateKey()
    {
        return true;
    }
}
