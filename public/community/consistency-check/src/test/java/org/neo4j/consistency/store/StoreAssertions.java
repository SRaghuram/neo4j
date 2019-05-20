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
package org.neo4j.consistency.store;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.consistency.ConsistencyCheckService;
import org.neo4j.consistency.checking.full.ConsistencyCheckIncompleteException;
import org.neo4j.internal.helpers.progress.ProgressMonitorFactory;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.logging.AssertableLogProvider;

import static org.junit.Assert.assertTrue;

public class StoreAssertions
{
    private StoreAssertions()
    {
    }

    public static void assertConsistentStore( DatabaseLayout databaseLayout ) throws ConsistencyCheckIncompleteException
    {
        Config configuration = Config.defaults( GraphDatabaseSettings.pagecache_memory, "8m" );
        AssertableLogProvider logger = new AssertableLogProvider();
        ConsistencyCheckService.Result result = new ConsistencyCheckService().runFullConsistencyCheck(
                databaseLayout, configuration, ProgressMonitorFactory.NONE, logger, false );

        assertTrue( "Consistency check for " + databaseLayout + " found inconsistencies:\n\n" + logger.serialize(),
                result.isSuccessful() );
    }
}
