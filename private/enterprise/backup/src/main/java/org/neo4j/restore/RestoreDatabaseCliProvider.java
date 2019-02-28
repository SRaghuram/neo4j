/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.restore;

import javax.annotation.Nonnull;

import org.neo4j.OnlineBackupCommandSection;
import org.neo4j.commandline.admin.AdminCommand;
import org.neo4j.commandline.admin.AdminCommandSection;
import org.neo4j.commandline.admin.CommandContext;
import org.neo4j.commandline.arguments.Arguments;

public class RestoreDatabaseCliProvider extends AdminCommand.Provider
{
    public RestoreDatabaseCliProvider()
    {
        super( "restore" );
    }

    @Override
    @Nonnull
    public Arguments allArguments()
    {
        return RestoreDatabaseCli.arguments();
    }

    @Override
    @Nonnull
    public String description()
    {
        return "Restore a backed up database.";
    }

    @Override
    @Nonnull
    public String summary()
    {
        return description();
    }

    @Override
    @Nonnull
    public AdminCommandSection commandSection()
    {
        return OnlineBackupCommandSection.instance();
    }

    @Override
    @Nonnull
    public AdminCommand create( CommandContext ctx )
    {
        return new RestoreDatabaseCli( ctx.getHomeDir(), ctx.getConfigDir() );
    }
}
