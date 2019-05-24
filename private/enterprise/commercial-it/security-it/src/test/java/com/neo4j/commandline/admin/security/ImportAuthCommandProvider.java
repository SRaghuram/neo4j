/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.commandline.admin.security;

import org.neo4j.annotations.service.ServiceProvider;
import org.neo4j.commandline.admin.AdminCommand;
import org.neo4j.commandline.admin.AdminCommandSection;
import org.neo4j.commandline.admin.CommandContext;
import org.neo4j.commandline.admin.security.AuthenticationCommandSection;
import org.neo4j.commandline.arguments.Arguments;

@ServiceProvider
public class ImportAuthCommandProvider implements AdminCommand.Provider
{
    @Override
    public String getName()
    {
        return ImportAuthCommand.COMMAND_NAME;
    }

    @Override
    public Arguments allArguments()
    {
        return ImportAuthCommand.arguments();
    }

    @Override
    public String description()
    {
        return "Import users and roles from flat files into the system graph, " +
                "for example when upgrading to Neo4j 3.5 Commercial Edition.";
    }

    @Override
    public String summary()
    {
        return "Import users and roles from files into the system graph.";
    }

    @Override
    public AdminCommandSection commandSection()
    {
        return AuthenticationCommandSection.instance();
    }

    @Override
    public AdminCommand create( CommandContext ctx )
    {
        return new ImportAuthCommand( ctx.getHomeDir(), ctx.getConfigDir(), ctx.getOutsideWorld() );
    }
}
