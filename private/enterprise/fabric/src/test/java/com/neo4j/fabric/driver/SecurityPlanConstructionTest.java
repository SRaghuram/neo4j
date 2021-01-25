/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.fabric.driver;

import com.neo4j.configuration.FabricEnterpriseConfig;
import org.junit.jupiter.api.Test;

import java.util.Map;

import org.neo4j.configuration.Config;
import org.neo4j.fabric.executor.Location;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.ssl.config.SslPolicyLoader;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@TestDirectoryExtension
class SecurityPlanConstructionTest
{
    @Inject
    private TestDirectory testDirectory;

    @Test
    void testNoFabricSslPolicy()
    {
        var properties = Map.of(
                "fabric.database.name", "mega",
                "fabric.graph.0.uri", "bolt://mega:1111"
        );
        var config = Config.newBuilder()
                .setRaw( properties )
                .build();

        var fabricConfig = FabricEnterpriseConfig.from( config );
        var sslLoader = SslPolicyLoader.create( config, NullLogProvider.nullLogProvider() );
        var driverConfigFactory = new ExternalDriverConfigFactory( fabricConfig, config, sslLoader );

        var securityPlan = driverConfigFactory.createSecurityPlan( new Location.Remote.External( 0, null, null, null ) );

        assertFalse( securityPlan.requiresEncryption() );
        assertNull(securityPlan.sslContext());
        assertFalse(securityPlan.requiresHostnameVerification());
    }

    @Test
    void testFabricSslPolicyDisabled()
    {
        var properties = Map.of(
                "fabric.database.name", "mega",
                "fabric.graph.0.uri", "bolt://mega:1111",
                "dbms.ssl.policy.fabric.enabled", "false"
        );
        var config = Config.newBuilder()
                .setRaw( properties )
                .build();

        var fabricConfig = FabricEnterpriseConfig.from( config );
        var sslLoader = SslPolicyLoader.create( config, NullLogProvider.nullLogProvider() );
        var driverConfigFactory = new ExternalDriverConfigFactory( fabricConfig, config, sslLoader );

        var securityPlan = driverConfigFactory.createSecurityPlan( new Location.Remote.External( 0, null, null, null ) );

        assertFalse(securityPlan.requiresEncryption());
        assertNull(securityPlan.sslContext());
        assertFalse(securityPlan.requiresHostnameVerification());
    }

    @Test
    void testProvidedFabricSslPolicy()
    {
        var properties = Map.of(
                "fabric.database.name", "mega",
                "fabric.graph.0.uri", "bolt://mega:1111",
                "fabric.graph.1.uri", "bolt://mega:2222",
                "fabric.graph.1.driver.ssl_enabled", "false",
                "dbms.ssl.policy.fabric.enabled", "true",
                "dbms.ssl.policy.fabric.verify_hostname", "true",
                "dbms.ssl.policy.fabric.base_directory", testDirectory.directory( "fabric-cert" ).toAbsolutePath().toString()
        );
        var config = Config.newBuilder()
                .setRaw( properties )
                .build();

        var fabricConfig = FabricEnterpriseConfig.from( config );
        var sslLoader = SslPolicyLoader.create( config, NullLogProvider.nullLogProvider() );
        var driverConfigFactory = new ExternalDriverConfigFactory( fabricConfig, config, sslLoader );

        var securityPlanForGraph0 = driverConfigFactory.createSecurityPlan( new Location.Remote.External( 0, null, null, null ) );

        assertTrue( securityPlanForGraph0.requiresEncryption() );
        assertNotNull( securityPlanForGraph0.sslContext() );
        assertTrue( securityPlanForGraph0.requiresHostnameVerification() );

        var securityPlanForGraph1 = driverConfigFactory.createSecurityPlan( new Location.Remote.External( 1, null, null, null ) );

        assertFalse(securityPlanForGraph1.requiresEncryption());
        assertNull(securityPlanForGraph1.sslContext());
        assertFalse(securityPlanForGraph1.requiresHostnameVerification());
    }
}
