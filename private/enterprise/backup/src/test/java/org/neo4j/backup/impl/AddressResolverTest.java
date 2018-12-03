/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.backup.impl;

import com.neo4j.kernel.impl.enterprise.configuration.OnlineBackupSettings;
import org.junit.Before;
import org.junit.Test;

import org.neo4j.helpers.AdvertisedSocketAddress;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.util.OptionalHostnamePort;

import static org.junit.Assert.assertEquals;

public class AddressResolverTest
{

    AddressResolver subject;

    @Before
    public void setup()
    {
        subject = new AddressResolver();
    }

    @Test
    public void noPortResolvesToDefault_cc()
    {
        Config config = Config.builder()
                .withSetting( OnlineBackupSettings.online_backup_listen_address, "any:1234" )
                .build();
        AdvertisedSocketAddress resolved = subject.resolveCorrectAddress( config, new OptionalHostnamePort( "localhost", null, null ) );

        // then
        assertEquals( 1234, resolved.getPort() );
    }
}
