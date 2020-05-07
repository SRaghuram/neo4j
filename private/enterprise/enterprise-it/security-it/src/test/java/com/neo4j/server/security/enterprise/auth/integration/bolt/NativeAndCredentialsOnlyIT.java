/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.security.enterprise.auth.integration.bolt;

import com.neo4j.server.security.enterprise.configuration.SecuritySettings;
import com.neo4j.test.extension.EnterpriseDbmsExtension;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.net.InetAddress;
import java.util.List;

import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.configuration.connectors.BoltConnector;
import org.neo4j.configuration.connectors.ConnectorPortRegister;
import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.ExtensionCallback;
import org.neo4j.test.extension.Inject;

import static com.neo4j.server.security.enterprise.auth.integration.bolt.DriverAuthHelper.assertAuth;
import static org.neo4j.configuration.connectors.BoltConnector.EncryptionLevel.DISABLED;

@EnterpriseDbmsExtension( configurationCallback = "configure" )
class NativeAndCredentialsOnlyIT
{
    @Inject
    private ConnectorPortRegister portRegister;

    private String boltUri;

    @ExtensionCallback
    void configure( TestDatabaseManagementServiceBuilder builder )
    {
        builder.setConfig( GraphDatabaseSettings.auth_enabled, true )
                .setConfig( BoltConnector.enabled, true )
                .setConfig( BoltConnector.encryption_level, DISABLED )
                .setConfig( BoltConnector.listen_address, new SocketAddress(  InetAddress.getLoopbackAddress().getHostAddress(), 0 ) )
                .setConfig( SecuritySettings.authentication_providers, List.of( SecuritySettings.NATIVE_REALM_NAME, "plugin-TestCredentialsOnlyPlugin" ) )
                .setConfig( SecuritySettings.authorization_providers, List.of( SecuritySettings.NATIVE_REALM_NAME, "plugin-TestCredentialsOnlyPlugin" ) );
    }

    @BeforeEach
    void setup()
    {
        boltUri = DriverAuthHelper.boltUri( portRegister );
    }

    @Test
    void shouldAuthenticateWithCredentialsOnlyPlugin()
    {
        assertAuth( boltUri, "", "BASE64-ENC-PASSWORD", "plugin-TestCredentialsOnlyPlugin" );
    }
}
