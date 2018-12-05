/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.backup.impl;

import java.nio.file.Path;
import javax.annotation.Nonnull;

import org.neo4j.OnlineBackupCommandSection;
import org.neo4j.commandline.admin.AdminCommand;
import org.neo4j.commandline.admin.AdminCommandSection;
import org.neo4j.commandline.admin.OutsideWorld;
import org.neo4j.commandline.arguments.Arguments;
import org.neo4j.helpers.progress.ProgressMonitorFactory;
import org.neo4j.logging.FormattedLogProvider;
import org.neo4j.logging.Level;
import org.neo4j.logging.LogProvider;

import static java.lang.String.format;

public class OnlineBackupCommandProvider extends AdminCommand.Provider
{
    public OnlineBackupCommandProvider()
    {
        super( "backup" );
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
    public AdminCommand create( Path homeDir, Path configDir, OutsideWorld outsideWorld )
    {
        boolean debug = System.getenv().get( "NEO4J_DEBUG") != null;
        LogProvider logProvider = FormattedLogProvider.withDefaultLogLevel( debug ? Level.DEBUG : Level.NONE ).toOutputStream( outsideWorld.outStream() );

        OnlineBackupContextFactory contextBuilder = new OnlineBackupContextFactory( homeDir, configDir );

        OnlineBackupExecutor backupExecutor = OnlineBackupExecutor.builder()
                .withOutputStream( outsideWorld.outStream() )
                .withFileSystem( outsideWorld.fileSystem() )
                .withLogProvider( logProvider )
                .withProgressMonitorFactory( ProgressMonitorFactory.textual( outsideWorld.errorStream() ) )
                .build();

        return new OnlineBackupCommand( outsideWorld, contextBuilder, backupExecutor );
    }
}
