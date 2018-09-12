/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
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
        return "Import users and roles from files into the system graph, " +
                "for example when upgrading to Neo4j 3.5 Commercial Edition. " +
                "This can be used to migrate auth data from the flat files used " +
                "as storage by the old native auth provider into the 'system-graph' auth provider.";
    }

    @Override
    @Nonnull
    public String summary()
    {
        return "Import users and roles from files into the system graph.";
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
