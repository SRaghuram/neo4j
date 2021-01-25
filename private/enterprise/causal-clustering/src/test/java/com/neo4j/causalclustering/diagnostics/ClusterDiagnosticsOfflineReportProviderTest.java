/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.diagnostics;

import org.assertj.core.util.Sets;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;

import org.neo4j.configuration.Config;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.EphemeralTestDirectoryExtension;

import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;

@EphemeralTestDirectoryExtension
class ClusterDiagnosticsOfflineReportProviderTest
{
    @Inject
    FileSystemAbstraction fs;

    @Test
    void returnsEmptyDiagnosticsForRaftIfClusterStateDoesNotExists()
    {
        // given:
        var provider = new ClusterDiagnosticsOfflineReportProvider();
        provider.init( fs, "some-db", Config.defaults(), Path.of( "some-path" ) );
        var raftSource = Sets.newTreeSet( "raft" );

        // when:
        var diagnostics = provider.provideSources( raftSource );

        // then:
        assertThat( diagnostics ).isEmpty();
    }

    @Test
    void returnsEmptyDiagnosticsForCcstateIfClusterStateDoesNotExists()
    {
        // given:
        var provider = new ClusterDiagnosticsOfflineReportProvider();
        provider.init( fs, "some-db", Config.defaults(), Path.of( "some-path" ) );
        var raftSource = Sets.newTreeSet( "ccstate" );

        // when:
        var diagnostics = provider.provideSources( raftSource );

        // then:
        assertThat( diagnostics ).isEmpty();
    }
}
