/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.harness.internal;

import com.neo4j.harness.EnterpriseTestServerBuilders;
import com.neo4j.kernel.impl.enterprise.configuration.OnlineBackupSettings;
import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.util.Arrays;

import org.neo4j.harness.ServerControls;
import org.neo4j.harness.TestServerBuilder;
import org.neo4j.kernel.configuration.Settings;
import org.neo4j.kernel.configuration.ssl.LegacySslPolicyConfig;
import org.neo4j.server.ServerTestUtils;
import org.neo4j.test.rule.SuppressOutput;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.test.server.HTTP;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;

public class EnterpriseInProcessServerBuilderIT
{
    @Rule
    public final TestDirectory testDir = TestDirectory.testDirectory();

    @Rule
    public final SuppressOutput suppressOutput = SuppressOutput.suppressAll();

    @Test
    public void shouldLaunchAServerInSpecifiedDirectory()
    {
        // Given
        File workDir = new File( testDir.directory(), "specific" );
        workDir.mkdir();

        // When
        try ( ServerControls server = getTestServerBuilder( workDir ).newServer() )
        {
            // Then
            assertThat( HTTP.GET( server.httpURI().toString() ).status(), equalTo( 200 ) );
            assertThat( workDir.list().length, equalTo( 1 ) );
        }

        // And after it's been closed, it should've cleaned up after itself.
        assertThat( Arrays.toString( workDir.list() ), workDir.list().length, equalTo( 0 ) );
    }

    private TestServerBuilder getTestServerBuilder( File workDir )
    {
        String certificatesDirectoryKey = LegacySslPolicyConfig.certificates_directory.name();
        String certificatesDirectoryValue = ServerTestUtils.getRelativePath(
                testDir.directory(),
                LegacySslPolicyConfig.certificates_directory
        );

        return EnterpriseTestServerBuilders.newInProcessBuilder( workDir )
                .withConfig( certificatesDirectoryKey, certificatesDirectoryValue )
                .withConfig( OnlineBackupSettings.online_backup_enabled, Settings.FALSE );
    }
}
