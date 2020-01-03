/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.kernel.impl.enterprise.configuration;

import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;

import org.neo4j.configuration.Config;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static com.neo4j.kernel.impl.enterprise.configuration.MetricsSettings.neoTransactionLogsEnabled;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.junit.jupiter.api.Assertions.assertEquals;

@TestDirectoryExtension
class MetricsSettingsMigratorTest
{
    @Inject
    private TestDirectory testDirectory;

    @Test
    void migrateSetting() throws IOException
    {
        File confFile = testDirectory.createFile( "neo4j.conf" );
        Files.write( confFile.toPath(), singletonList( "metrics.neo4j.logrotation.enabled=false" ) );

        Config config = Config.newBuilder().fromFile( confFile ).build();

        assertEquals( false, config.get( neoTransactionLogsEnabled ) );
    }

    @Test
    void overriddenSettingMigration() throws IOException
    {
        File confFile = testDirectory.createFile( "neo4j.conf" );
        Files.write( confFile.toPath(), asList( "metrics.neo4j.logrotation.enabled=false",
                neoTransactionLogsEnabled.name() + "=true" ) );

        Config config = Config.newBuilder().fromFile( confFile ).build();

        assertEquals( true, config.get( neoTransactionLogsEnabled ) );
    }
}
