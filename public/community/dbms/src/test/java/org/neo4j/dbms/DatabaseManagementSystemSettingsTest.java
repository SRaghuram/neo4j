/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.dbms;

import org.junit.jupiter.api.Test;

import java.io.File;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsEqual.equalTo;

class DatabaseManagementSystemSettingsTest
{
    @Test
    void shouldPutDatabasesDirectoriesIntoData()
    {
        Config config = Config.defaults( GraphDatabaseSettings.data_directory, "the-data-directory" );
        assertThat( config.get( GraphDatabaseSettings.databases_root_path ), equalTo( new File( "the-data-directory/databases/" ) ) );
    }
}
