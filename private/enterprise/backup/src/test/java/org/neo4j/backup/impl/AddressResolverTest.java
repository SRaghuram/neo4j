/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.backup.impl;

import org.junit.Before;
import org.junit.Test;

import org.neo4j.helpers.AdvertisedSocketAddress;
import org.neo4j.helpers.HostnamePort;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.enterprise.configuration.OnlineBackupSettings;
import org.neo4j.kernel.impl.util.OptionalHostnamePort;

import static org.junit.Assert.assertEquals;

public class AddressResolverTest
{

    AddressResolver subject;

    // Parameters
    Config defaultConfig = Config.defaults();

    @Before
    public void setup()
    {
        subject = new AddressResolver();
    }

    @Test
    public void noPortResolvesToDefault_ha()
    {
        // given
        Integer portIsNotSupplied = null;

        // when
        HostnamePort resolved = subject.resolveCorrectHAAddress( defaultConfig, new OptionalHostnamePort( "localhost", portIsNotSupplied, null ) );

        // then
        assertEquals( 6362, resolved.getPort() );
    }

    @Test
    public void noPortResolvesToDefault_cc()
    {
        Config config = Config.builder()
                .withSetting( OnlineBackupSettings.online_backup_server, "any:1234" )
                .build();
        AdvertisedSocketAddress resolved = subject.resolveCorrectCCAddress( config, new OptionalHostnamePort( "localhost", null, null ) );

        // then
        assertEquals( 1234, resolved.getPort() );
    }

    @Test
    public void suppliedPortResolvesToGiven_ha()
    {
        // given
        Integer portIsSupplied = 1234;

        // when
        HostnamePort resolved = subject.resolveCorrectHAAddress( defaultConfig, new OptionalHostnamePort( "localhost", portIsSupplied, null ) );

        // then
        assertEquals( resolved.getPort(), portIsSupplied.intValue() );
    }
}
