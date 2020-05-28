/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.harness.internal;

import com.neo4j.configuration.OnlineBackupSettings;
import com.neo4j.harness.EnterpriseNeo4jBuilders;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.junit.jupiter.api.parallel.Resources;

import java.io.File;
import java.util.Arrays;

import org.neo4j.harness.Neo4j;
import org.neo4j.harness.Neo4jBuilder;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.SuppressOutputExtension;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.test.server.HTTP;

import static org.assertj.core.api.Assertions.assertThat;

@TestDirectoryExtension
@ExtendWith( SuppressOutputExtension.class )
@ResourceLock( Resources.SYSTEM_OUT )
class EnterpriseInProcessServerBuilderIT
{
    @Inject
    private TestDirectory testDir;

    @Test
    void shouldLaunchAServerInSpecifiedDirectory()
    {
        // Given
        File workDir = testDir.directory("specific" );

        // When
        try ( Neo4j server = getTestServerBuilder( workDir ).build() )
        {
            // Then
            assertThat( HTTP.GET( server.httpURI().toString() ).status() ).isEqualTo( 200 );
            assertThat( workDir.list().length ).isEqualTo( 1 );
        }

        // And after it's been closed, it should've cleaned up after itself.
        assertThat( workDir.list().length ).as( Arrays.toString( workDir.list() ) ).isEqualTo( 0 );
    }

    private Neo4jBuilder getTestServerBuilder( File workDir )
    {
        return EnterpriseNeo4jBuilders.newInProcessBuilder( workDir )
                                          .withConfig( OnlineBackupSettings.online_backup_enabled, false );
    }
}
