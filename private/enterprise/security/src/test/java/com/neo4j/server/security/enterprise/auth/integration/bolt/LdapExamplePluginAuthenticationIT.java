/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.security.enterprise.auth.integration.bolt;

import com.neo4j.server.security.enterprise.auth.plugin.LdapGroupHasUsersAuthPlugin;
import com.neo4j.server.security.enterprise.configuration.SecuritySettings;
import org.apache.directory.server.annotations.CreateLdapServer;
import org.apache.directory.server.annotations.CreateTransport;
import org.apache.directory.server.annotations.SaslMechanism;
import org.apache.directory.server.core.annotations.ApplyLdifFiles;
import org.apache.directory.server.core.annotations.ContextEntry;
import org.apache.directory.server.core.annotations.CreateDS;
import org.apache.directory.server.core.annotations.CreatePartition;
import org.apache.directory.server.core.annotations.LoadSchema;
import org.apache.directory.server.core.integ.CreateLdapServerRule;
import org.apache.directory.server.ldap.LdapServer;
import org.apache.directory.server.ldap.handlers.extended.StartTlsHandler;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.Collections;
import java.util.Map;

import org.neo4j.driver.v1.Driver;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.server.security.enterprise.auth.plugin.api.PredefinedRoles;

import static org.neo4j.helpers.collection.MapUtil.map;

@CreateDS(
        name = "PluginAuthTest",
        partitions = { @CreatePartition(
                name = "example",
                suffix = "dc=example,dc=com",
                contextEntry = @ContextEntry( entryLdif = "dn: dc=example,dc=com\n" +
                                                          "dc: example\n" +
                                                          "o: example\n" +
                                                          "objectClass: top\n" +
                                                          "objectClass: dcObject\n" +
                                                          "objectClass: organization\n\n" ) ),
        },
        loadedSchemas = {
                @LoadSchema( name = "nis", enabled = true ),
        } )
@CreateLdapServer(
        transports = { @CreateTransport( protocol = "LDAP", address = "0.0.0.0" ),
                @CreateTransport( protocol = "LDAPS", address = "0.0.0.0", ssl = true )
        },

        saslMechanisms = {
                @SaslMechanism( name = "DIGEST-MD5", implClass = org.apache.directory.server.ldap.handlers.sasl
                        .digestMD5.DigestMd5MechanismHandler.class ),
                @SaslMechanism( name  = "CRAM-MD5", implClass = org.apache.directory.server.ldap.handlers.sasl
                        .cramMD5.CramMd5MechanismHandler.class )
        },
        saslHost = "0.0.0.0",
        extendedOpHandlers = { StartTlsHandler.class },
        keyStore = "target/test-classes/neo4j_ldap_test_keystore.jks",
        certificatePassword = "secret"
)
@ApplyLdifFiles( "ldap_group_has_users_test_data.ldif" )
public class LdapExamplePluginAuthenticationIT extends EnterpriseAuthenticationTestBase
{
    @ClassRule
    public static CreateLdapServerRule ldapServerRule = new CreateLdapServerRule();

    @Before
    @Override
    public void setup() throws Exception
    {
        super.setup();
        LdapServer ldapServer = ldapServerRule.getLdapServer();
        ldapServer.setConfidentialityRequired( false );
        checkIfLdapServerIsReachable(ldapServer.getSaslHost(), ldapServer.getPort());
    }

    @Override
    protected Map<Setting<?>, String> getSettings()
    {
        return Collections.singletonMap( SecuritySettings.auth_provider, SecuritySettings.PLUGIN_REALM_NAME_PREFIX + new LdapGroupHasUsersAuthPlugin().name() );
    }

    @Test
    public void shouldBeAbleToLoginAndAuthorizeWithLdapGroupHasUsersAuthPlugin()
    {
        Map<String,Object> parameters = map( "port", ldapServerRule.getLdapServer().getPort() );

        try ( Driver driver = connectDriverWithParameters( "neo", "abc123", parameters ) )
        {
            assertRoles( driver, PredefinedRoles.READER );
        }

        try ( Driver driver = connectDriverWithParameters( "tank", "abc123", parameters ) )
        {
            assertRoles( driver, PredefinedRoles.PUBLISHER );
        }

        try ( Driver driver = connectDriverWithParameters( "smith", "abc123", parameters ) )
        {
            assertRoles( driver );
        }
    }
}
