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
import java.util.List;

import org.neo4j.configuration.Config;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static com.neo4j.configuration.EnterpriseEditionSettings.dynamic_setting_allowlist;
import static java.util.Collections.singletonList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.logging.AssertableLogProvider.Level.WARN;
import static org.neo4j.logging.LogAssertions.assertThat;

@TestDirectoryExtension
class EnterpriseEditionSettingsMigratorTest
{
    @Inject
    private TestDirectory testDirectory;

    @Test
    void migrateWhitelistSetting() throws IOException
    {
        Path confFile = testDirectory.createFile( "neo4j.conf" );
        Files.write( confFile, singletonList( "dbms.dynamic.setting.whitelist=a,b" ) );

        Config config = Config.newBuilder().fromFile( confFile ).build();

        var logProvider = new AssertableLogProvider();
        config.setLogger( logProvider.getLog( Config.class ) );

        assertThat( logProvider ).forClass( Config.class ).forLevel( WARN )
                .containsMessageWithArguments( "Use of deprecated setting %s. It is replaced by %s", "dbms.dynamic.setting.whitelist",
                        dynamic_setting_allowlist.name() );

        assertEquals( List.of( "a", "b" ), config.get( dynamic_setting_allowlist ) );
    }
}
