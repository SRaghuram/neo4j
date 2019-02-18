/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.harness.internal;

import com.neo4j.harness.internal.CommercialTestNeo4jBuilders;
import com.neo4j.kernel.impl.enterprise.configuration.OnlineBackupSettings;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.File;
import java.util.Arrays;

import org.neo4j.configuration.LegacySslPolicyConfig;
import org.neo4j.configuration.Settings;
import org.neo4j.server.ServerTestUtils;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.SuppressOutputExtension;
import org.neo4j.test.extension.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.test.server.HTTP;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

@ExtendWith( {TestDirectoryExtension.class, SuppressOutputExtension.class} )
class CommercialInProcessServerBuilderIT
{
    @Inject
    private TestDirectory testDir;

    @Test
    void shouldLaunchAServerInSpecifiedDirectory()
    {
        // Given
        File workDir = testDir.directory("specific" );

        // When
        try ( InProcessNeo4j server = getTestServerBuilder( workDir ).build() )
        {
            // Then
            assertThat( HTTP.GET( server.httpURI().toString() ).status(), equalTo( 200 ) );
            assertThat( workDir.list().length, equalTo( 1 ) );
        }

        // And after it's been closed, it should've cleaned up after itself.
        assertThat( Arrays.toString( workDir.list() ), workDir.list().length, equalTo( 0 ) );
    }

    private Neo4jBuilder getTestServerBuilder( File workDir )
    {
        String certificatesDirectoryKey = LegacySslPolicyConfig.certificates_directory.name();
        String certificatesDirectoryValue = ServerTestUtils.getRelativePath(
                testDir.directory(),
                LegacySslPolicyConfig.certificates_directory
        );

        return CommercialTestNeo4jBuilders.newInProcessBuilder( workDir )
                .withConfig( certificatesDirectoryKey, certificatesDirectoryValue )
                .withConfig( OnlineBackupSettings.online_backup_enabled, Settings.FALSE );
    }
}
