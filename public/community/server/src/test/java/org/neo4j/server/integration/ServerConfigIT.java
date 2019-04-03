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
package org.neo4j.server.integration;

import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import javax.management.ObjectName;

import org.neo4j.configuration.ExternalSettings;
import org.neo4j.jmx.impl.ConfigurationBean;
import org.neo4j.server.CommunityNeoServer;
import org.neo4j.server.helpers.CommunityServerBuilder;
import org.neo4j.test.server.ExclusiveServerTestBase;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.neo4j.configuration.GraphDatabaseSettings.transaction_timeout;
import static org.neo4j.jmx.JmxUtils.getAttribute;
import static org.neo4j.jmx.JmxUtils.getObjectName;

public class ServerConfigIT extends ExclusiveServerTestBase
{
    @Rule
    public TemporaryFolder tempDir = new TemporaryFolder();

    private CommunityNeoServer server;

    @Test
    public void durationsAlwaysHaveUnitsInJMX() throws Throwable
    {
        // Given
        server = CommunityServerBuilder.serverOnRandomPorts()
                .withProperty( transaction_timeout.name(), "10" )
                .build();

        // When
        server.start();

        // Then
        ObjectName name = getObjectName( server.getDatabase().getGraph(), ConfigurationBean.CONFIGURATION_MBEAN_NAME );
        String attr = getAttribute( name, transaction_timeout.name() );
        assertThat( attr, equalTo( "10000ms" ) );
    }

    @Test
    public void serverConfigShouldBeVisibleInJMX() throws Throwable
    {
        // Given
        String configValue = tempDir.newFile().getAbsolutePath();
        server = CommunityServerBuilder.serverOnRandomPorts().withProperty(
        ExternalSettings.run_directory.name(), configValue ).build();

        // When
        server.start();

        // Then
        ObjectName name = getObjectName( server.getDatabase().getGraph(), ConfigurationBean.CONFIGURATION_MBEAN_NAME );
        String attr = getAttribute( name, ExternalSettings.run_directory.name() );
        assertThat( attr, equalTo( configValue ) );
    }

    @After
    public void cleanup()
    {
        server.stop();
    }
}
