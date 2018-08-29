/*
 * Copyright (c) 2002-2018 "Neo4j,"
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
package com.neo4j.commandline.admin.security;

import java.nio.file.Path;
import javax.annotation.Nonnull;

import org.neo4j.commandline.admin.AdminCommand;
import org.neo4j.commandline.admin.AdminCommandSection;
import org.neo4j.commandline.admin.OutsideWorld;
import org.neo4j.commandline.admin.security.AuthenticationCommandSection;
import org.neo4j.commandline.arguments.Arguments;

public class ImportAuthCommandProvider extends AdminCommand.Provider
{
    public ImportAuthCommandProvider()
    {
        super( ImportAuthCommand.COMMAND_NAME );
    }

    @Override
    @Nonnull
    public Arguments allArguments()
    {
        return ImportAuthCommand.arguments();
    }

    @Override
    @Nonnull
    public String description()
    {
        return "Import users and roles from files into system graph, " +
                "for example when upgrading to neo4j 3.5 commercial.";
    }

    @Override
    @Nonnull
    public String summary()
    {
        return "Import users and roles from files into system graph.";
    }

    @Override
    @Nonnull
    public AdminCommandSection commandSection()
    {
        return AuthenticationCommandSection.instance();
    }

    @Override
    @Nonnull
    public AdminCommand create( Path homeDir, Path configDir, OutsideWorld outsideWorld )
    {
        return new ImportAuthCommand( homeDir, configDir, outsideWorld );
    }
}
