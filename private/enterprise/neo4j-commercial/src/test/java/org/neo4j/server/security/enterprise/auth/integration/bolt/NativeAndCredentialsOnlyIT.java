/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.server.security.enterprise.auth.integration.bolt;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;

import java.net.InetAddress;
import java.util.Collections;
import java.util.Map;

import org.neo4j.graphdb.config.Setting;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.kernel.configuration.BoltConnector;
import org.neo4j.server.security.enterprise.configuration.SecuritySettings;
import org.neo4j.test.rule.DatabaseRule;
import org.neo4j.test.rule.EnterpriseDatabaseRule;
import org.neo4j.test.rule.TestDirectory;

import static org.neo4j.kernel.configuration.BoltConnector.EncryptionLevel.OPTIONAL;
import static org.neo4j.server.security.enterprise.auth.integration.bolt.DriverAuthHelper.assertAuth;
import static org.neo4j.server.security.enterprise.auth.integration.bolt.DriverAuthHelper.boltUri;

public class NativeAndCredentialsOnlyIT
{
    private TestDirectory testDirectory = TestDirectory.testDirectory( getClass() );

    private DatabaseRule dbRule = getDatabaseTestRule( testDirectory );

    @Rule
    public RuleChain chain = RuleChain.outerRule( testDirectory ).around( dbRule );

    @Before
    public void setup() throws Exception
    {
        String host = InetAddress.getLoopbackAddress().getHostAddress() + ":0";
        dbRule.withSetting( GraphDatabaseSettings.auth_enabled, "true" )
                .withSetting( new BoltConnector( "bolt" ).type, "BOLT" )
                .withSetting( new BoltConnector( "bolt" ).enabled, "true" )
                .withSetting( new BoltConnector( "bolt" ).encryption_level, OPTIONAL.name() )
                .withSetting( new BoltConnector( "bolt" ).listen_address, host );
        dbRule.withSettings( getSettings() );
        dbRule.ensureStarted();
    }

    protected Map<Setting<?>, String> getSettings()
    {
        return Collections.singletonMap( SecuritySettings.auth_providers, "native,plugin-TestCredentialsOnlyPlugin" );
    }

    protected DatabaseRule getDatabaseTestRule( TestDirectory testDirectory )
    {
        return new EnterpriseDatabaseRule( testDirectory ).startLazily();
    }

    @Test
    public void shouldAuthenticateWithCredentialsOnlyPlugin()
    {
        assertAuth( boltUri( dbRule ), "", "BASE64-ENC-PASSWORD", "plugin-TestCredentialsOnlyPlugin" );
    }
}
