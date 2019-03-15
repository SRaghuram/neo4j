/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.commandline.dbms;

import javax.annotation.Nonnull;

import org.neo4j.annotations.service.ServiceProvider;
import org.neo4j.commandline.admin.AdminCommand;
import org.neo4j.commandline.admin.AdminCommandSection;
import org.neo4j.commandline.admin.CommandContext;
import org.neo4j.commandline.arguments.Arguments;

@ServiceProvider
public class UnbindFromClusterCommandProvider implements AdminCommand.Provider
{

    @Nonnull
    @Override
    public String getName()
    {
        return "unbind";
    }

    @Override
    @Nonnull
    public Arguments allArguments()
    {
        return UnbindFromClusterCommand.arguments();
    }

    @Override
    @Nonnull
    public String summary()
    {
        return "Removes cluster state data for the specified database.";
    }

    @Override
    @Nonnull
    public AdminCommandSection commandSection()
    {
        return ClusteringCommandSection.instance();
    }

    @Override
    @Nonnull
    public String description()
    {
        return "Removes cluster state data for the specified database, so that the " +
                "instance can rebind to a new or recovered cluster.";
    }

    @Override
    @Nonnull
    public AdminCommand create( CommandContext ctx )
    {
        return new UnbindFromClusterCommand( ctx.getHomeDir(), ctx.getConfigDir(), ctx.getOutsideWorld() );
    }
}
