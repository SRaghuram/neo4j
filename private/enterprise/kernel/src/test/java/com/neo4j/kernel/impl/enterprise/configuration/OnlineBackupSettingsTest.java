/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.kernel.impl.enterprise.configuration;

import org.junit.jupiter.api.Test;

import org.neo4j.helpers.ListenSocketAddress;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.logging.AssertableLogProvider;

import static com.neo4j.kernel.impl.enterprise.configuration.OnlineBackupSettings.online_backup_listen_address;
import static org.hamcrest.Matchers.containsString;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.neo4j.logging.AssertableLogProvider.inLog;

class OnlineBackupSettingsTest
{
    @Test
    void shouldMigrateOldBackupAddressSettingWithHostnameAndPort()
    {
        testBackupAddressSettingMigration( "neo4j.com:911", "neo4j.com", 911 );
    }

    @Test
    void shouldMigrateOldBackupAddressSettingWithOnlyPort()
    {
        testBackupAddressSettingMigration( ":8912", "127.0.0.1", 8912 );
    }

    @Test
    void shouldNotMigrateOldBackupAddressSettingWhenEmpty()
    {
        testBackupAddressSettingMigration( "", "127.0.0.1", 6362 );
    }

    private static void testBackupAddressSettingMigration( String value, String expectedHostname, int expectedPort )
    {
        Config config = Config.builder()
                .withSetting( "dbms.backup.address", value )
                .build();

        AssertableLogProvider logProvider = new AssertableLogProvider();
        config.setLogger( logProvider.getLog( Config.class ) );

        assertFalse( config.getRaw( "dbms.backup.address" ).isPresent(), "Old backup address setting should be absent" );
        assertEquals( new ListenSocketAddress( expectedHostname, expectedPort ), config.get( online_backup_listen_address ) );

        logProvider.assertAtLeastOnce(
                inLog( Config.class ).warn( containsString( "Deprecated configuration options used" ) ) );
        logProvider.assertAtLeastOnce(
                inLog( Config.class ).warn( containsString( "dbms.backup.address has been replaced with dbms.backup.listen_address" ) ) );
    }
}
