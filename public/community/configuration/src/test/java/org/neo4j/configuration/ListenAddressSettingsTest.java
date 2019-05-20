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
package org.neo4j.configuration;

import org.junit.jupiter.api.Test;

import java.util.Map;

import org.neo4j.graphdb.config.Setting;
import org.neo4j.internal.helpers.ListenSocketAddress;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.configuration.Settings.legacyFallback;
import static org.neo4j.configuration.Settings.listenAddress;
import static org.neo4j.internal.helpers.collection.MapUtil.stringMap;

class ListenAddressSettingsTest
{
    private static final Setting<ListenSocketAddress> legacy_address_setting = listenAddress( "address", 1234 );
    private static final Setting<ListenSocketAddress> listen_address_setting =
            legacyFallback( legacy_address_setting, listenAddress( "listen_address", 1234 ) );

    @Test
    void shouldParseExplicitSettingValueWhenProvided()
    {
        // given
        Map<String, String> config = stringMap(
                GraphDatabaseSettings.default_listen_address.name(), "server1.example.com",
                listen_address_setting.name(), "server1.internal:4000" );

        // when
        ListenSocketAddress listenSocketAddress = listen_address_setting.apply( config::get );

        // then
        assertEquals( "server1.internal", listenSocketAddress.getHostname() );
        assertEquals( 4000, listenSocketAddress.getPort() );
    }

    @Test
    void shouldCombineDefaultHostnameWithSettingSpecificPortWhenNoValueProvided()
    {
        // given
        Map<String, String> config = stringMap(
                GraphDatabaseSettings.default_listen_address.name(), "server1.example.com" );

        // when
        ListenSocketAddress listenSocketAddress = listen_address_setting.apply( config::get );

        // then
        assertEquals( "server1.example.com", listenSocketAddress.getHostname() );
        assertEquals( 1234, listenSocketAddress.getPort() );
    }

    @Test
    void shouldCombineDefaultHostnameWithExplicitPortWhenOnlyAPortProvided()
    {
        // given
        Map<String, String> config = stringMap(
                GraphDatabaseSettings.default_listen_address.name(), "server1.example.com",
                listen_address_setting.name(), ":4000" );

        // when
        ListenSocketAddress listenSocketAddress = listen_address_setting.apply( config::get );

        // then
        assertEquals( "server1.example.com", listenSocketAddress.getHostname() );
        assertEquals( 4000, listenSocketAddress.getPort() );
    }
}
