/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.diagnostics.providers;

import org.junit.jupiter.api.Test;

import java.io.File;

import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static org.assertj.core.api.Assertions.assertThat;

@TestDirectoryExtension
class SystemDiagnosticsTest
{
    @Inject
    private TestDirectory directory;

    @Test
    void shouldHandleCanonicalizingWildcardPaths()
    {
        // given
        String path = directory.directory( "mydir" ) + File.separator + "*";

        // when
        String canonicalized = SystemDiagnostics.canonicalize( path );

        // then
        assertThat( canonicalized ).isEqualTo( path );
    }
}
