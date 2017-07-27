/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.shell.impl;

import org.neo4j.helpers.Service;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.extension.KernelExtensionFactory;
import org.neo4j.kernel.impl.spi.KernelContext;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.shell.ShellSettings;

@Service.Implementation(KernelExtensionFactory.class)
public final class ShellServerExtensionFactory extends KernelExtensionFactory<ShellServerExtensionFactory.Dependencies>
{
    static final String KEY = "shell";

    public interface Dependencies
    {
        Config getConfig();

        GraphDatabaseAPI getGraphDatabaseAPI();
    }

    public ShellServerExtensionFactory()
    {
        super( KEY );
    }

    @Override
    public Class<ShellSettings> getSettingsClass()
    {
        return ShellSettings.class;
    }

    @Override
    public Lifecycle newInstance( KernelContext context, Dependencies dependencies ) throws Throwable
    {
        return new ShellServerKernelExtension( dependencies.getConfig(), dependencies.getGraphDatabaseAPI() );
    }
}
