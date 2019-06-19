/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.backup.impl;

import com.neo4j.OnlineBackupCommandSection;

import javax.annotation.Nonnull;

import org.neo4j.annotations.service.ServiceProvider;
import org.neo4j.commandline.admin.AdminCommand;
import org.neo4j.commandline.admin.AdminCommandSection;
import org.neo4j.commandline.admin.CommandContext;
import org.neo4j.commandline.admin.OutsideWorld;
import org.neo4j.commandline.arguments.Arguments;
import org.neo4j.internal.helpers.progress.ProgressMonitorFactory;
import org.neo4j.logging.FormattedLogProvider;
import org.neo4j.logging.Level;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.NullLogProvider;

import static java.lang.String.format;

@ServiceProvider
public class OnlineBackupCommandProvider implements AdminCommand.Provider
{
    @Nonnull
    @Override
    public String getName()
    {
        return "backup";
    }

    @Override
    @Nonnull
    public Arguments allArguments()
    {
        return OnlineBackupContextFactory.arguments();
    }

    @Override
    @Nonnull
    public String description()
    {
        return format( "Perform an online backup from a running Neo4j enterprise server. Neo4j's backup service must " +
                "have been configured on the server beforehand.%n" +
                "%n" +
                "All consistency checks except 'cc-graph' can be quite expensive so it may be useful to turn them off" +
                " for very large databases. Increasing the heap size can also be a good idea." +
                " See 'neo4j-admin help' for details.%n" +
                "%n" +
                "For more information see: https://neo4j.com/docs/operations-manual/current/backup/" );
    }

    @Override
    @Nonnull
    public String summary()
    {
        return "Perform an online backup from a running Neo4j enterprise server.";
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
        OutsideWorld outsideWorld = ctx.getOutsideWorld();
        LogProvider userLogProvider = FormattedLogProvider.toOutputStream( outsideWorld.outStream() );
        LogProvider internalLogProvider = buildInternalLogProvider( ctx, outsideWorld );

        OnlineBackupContextFactory contextBuilder = new OnlineBackupContextFactory( ctx.getConfigDir() );

        OnlineBackupExecutor backupExecutor = OnlineBackupExecutor.builder()
                .withFileSystem( outsideWorld.fileSystem() )
                .withUserLogProvider( userLogProvider )
                .withInternalLogProvider( internalLogProvider )
                .withProgressMonitorFactory( ProgressMonitorFactory.textual( outsideWorld.errorStream() ) )
                .build();

        return new OnlineBackupCommand( outsideWorld, contextBuilder, backupExecutor );
    }

    private LogProvider buildInternalLogProvider( CommandContext ctx, OutsideWorld outsideWorld )
    {
        if ( ctx.isDebugEnabled() )
        {
            return FormattedLogProvider.withDefaultLogLevel( Level.DEBUG ).toOutputStream( outsideWorld.outStream() );
        }
        return NullLogProvider.getInstance();
    }
}
