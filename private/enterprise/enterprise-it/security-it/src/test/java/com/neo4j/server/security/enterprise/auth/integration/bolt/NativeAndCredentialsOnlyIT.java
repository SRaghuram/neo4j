/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.security.enterprise.auth.integration.bolt;

import com.neo4j.server.security.enterprise.configuration.SecuritySettings;
import com.neo4j.test.rule.EnterpriseDbmsRule;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;

import java.net.InetAddress;
import java.util.List;

import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.configuration.connectors.BoltConnector;
import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.TestDirectory;

import static com.neo4j.server.security.enterprise.auth.integration.bolt.DriverAuthHelper.assertAuth;
import static org.neo4j.configuration.connectors.BoltConnector.EncryptionLevel.DISABLED;

public class NativeAndCredentialsOnlyIT
{
    private final TestDirectory testDirectory = TestDirectory.testDirectory();

    private DbmsRule dbRule = new EnterpriseDbmsRule( testDirectory ).startLazily();

    @Rule
    public RuleChain chain = RuleChain.outerRule( testDirectory ).around( dbRule );

    private String boltUri;

    @Before
    public void setup()
    {
        dbRule.withSetting( GraphDatabaseSettings.auth_enabled, true )
                .withSetting( BoltConnector.enabled, true )
                .withSetting( BoltConnector.encryption_level, DISABLED )
                .withSetting( BoltConnector.listen_address, new SocketAddress(  InetAddress.getLoopbackAddress().getHostAddress(), 0 ) )
                .withSetting( SecuritySettings.authentication_providers, List.of( SecuritySettings.NATIVE_REALM_NAME, "plugin-TestCredentialsOnlyPlugin" ) )
                .withSetting( SecuritySettings.authorization_providers, List.of( SecuritySettings.NATIVE_REALM_NAME, "plugin-TestCredentialsOnlyPlugin" ) );
        dbRule.ensureStarted();
        boltUri = DriverAuthHelper.boltUri( dbRule );
    }

    @Test
    public void shouldAuthenticateWithCredentialsOnlyPlugin()
    {
        assertAuth( boltUri, "", "BASE64-ENC-PASSWORD", "plugin-TestCredentialsOnlyPlugin" );
    }
}
