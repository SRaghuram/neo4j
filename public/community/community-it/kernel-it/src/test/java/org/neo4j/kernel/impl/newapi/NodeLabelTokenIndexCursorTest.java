/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.kernel.impl.newapi;

import org.junit.jupiter.api.Disabled;

import org.neo4j.kernel.impl.index.schema.RelationshipTypeScanStoreSettings;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;

@Disabled( "Scan stores as token indexes are not fully implemented yet" )
public class NodeLabelTokenIndexCursorTest extends NodeLabelIndexCursorTestBase<WriteTestSupport>
{
    @Override
    public WriteTestSupport newTestSupport()
    {
        return new WriteTestSupport()
        {
            @Override
            protected TestDatabaseManagementServiceBuilder configure( TestDatabaseManagementServiceBuilder builder )
            {
                builder = builder.setConfig( RelationshipTypeScanStoreSettings.enable_scan_stores_as_token_indexes, true );
                return super.configure( builder );
            }
        };
    }
}
