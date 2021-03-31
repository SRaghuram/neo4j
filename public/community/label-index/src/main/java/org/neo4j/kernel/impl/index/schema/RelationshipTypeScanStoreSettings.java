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
package org.neo4j.kernel.impl.index.schema;

import org.neo4j.annotations.service.ServiceProvider;
import org.neo4j.configuration.Description;
import org.neo4j.configuration.Internal;
import org.neo4j.configuration.SettingsDeclaration;
import org.neo4j.graphdb.config.Setting;

import static org.neo4j.configuration.SettingImpl.newBuilder;
import static org.neo4j.configuration.SettingValueParsers.BOOL;

@ServiceProvider
public class RelationshipTypeScanStoreSettings implements SettingsDeclaration
{
    @Description( "Decide if relationship type scan store should be enabled or not. This setting might compromise rolling upgrade. " +
                  "This setting is incompatible with enable_scan_stores_as_token_indexes." )
    @Internal
    public static final Setting<Boolean> enable_relationship_type_scan_store =
            newBuilder( "unsupported.dbms.enable_relationship_type_scan_store", BOOL, false ).build();

    @Description( "Decide if scan stores should be treated as token indexes or not. This setting should just be used for testing with the " +
                  "new functionality until it is complete." )
    @Internal
    public static final Setting<Boolean> enable_scan_stores_as_token_indexes =
            newBuilder( "unsupported.dbms.enable_scan_stores_as_token_indexes", BOOL, false ).build();

    @Description( "Decide if property indexes on relationships should be enabled or not. This setting should just be used for testing with the " +
                  "new functionality until it is complete." )
    @Internal
    public static final Setting<Boolean> enable_relationship_property_indexes =
            newBuilder( "unsupported.dbms.enable_relationship_property_indexes", BOOL, false ).build();
}
