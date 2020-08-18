/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.kernel.impl.enterprise.configuration;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static com.neo4j.configuration.OnlineBackupSettings.online_backup_listen_address;
import static org.junit.jupiter.api.Assertions.assertEquals;

@TestDirectoryExtension
class OnlineBackupSettingsMigratorTest
{
    @Inject
    private TestDirectory testDirectory;

    @Test
    void testMigration() throws IOException
    {
        Path confFile = testDirectory.createFilePath( "test.conf" );
        Files.write( confFile, Collections.singletonList( "dbms.backup.address=foo:123" ) );

        Config config = Config.newBuilder().fromFile( confFile ).build();

        assertEquals( new SocketAddress( "foo", 123 ), config.get( online_backup_listen_address ) );
    }

    @Test
    void testMigrationOverriddenMigration() throws IOException
    {
        Path confFile = testDirectory.createFilePath( "test.conf" );
        Files.write( confFile, Arrays.asList( "dbms.backup.address=foo:123", online_backup_listen_address.name() + "=bar:567" ) );

        Config config = Config.newBuilder().fromFile( confFile ).build();

        assertEquals( new SocketAddress( "bar", 567 ), config.get( online_backup_listen_address ) );
    }
}
