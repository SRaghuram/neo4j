/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.dbms.database;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Instant;
import java.util.Objects;
import java.util.function.Predicate;

import org.neo4j.commandline.dbms.DumpCommand;
import org.neo4j.configuration.Config;
import org.neo4j.dbms.api.DatabaseManagementException;
import org.neo4j.dbms.archive.Dumper;
import org.neo4j.dbms.database.DatabaseContext;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.util.Id;

import static java.lang.String.format;
import static org.neo4j.configuration.GraphDatabaseSettings.database_dumps_root_path;
import static org.neo4j.dbms.archive.CompressionFormat.selectCompressionFormat;

/**
 * Simple wrapper over {@link Dumper} to enable producing database dumps as part of DROP DATABASE commands
 * rather than via neo4j-admin and {@link DumpCommand}.
 */
class RuntimeDatabaseDumper extends LifecycleAdapter
{
    private final Clock clock;
    private final Path dumpsRoot;
    private FileSystemAbstraction fsa;
    private volatile boolean started;

    RuntimeDatabaseDumper( Clock clock, Config config, FileSystemAbstraction fsa )
    {
        this.clock = clock;
        this.dumpsRoot = config.get( database_dumps_root_path );
        this.fsa = fsa;
        this.started = false;
    }

    @Override
    public void start() throws Exception
    {
        fsa.mkdirs( dumpsRoot );
        started = true;
    }

    @Override
    public void stop()
    {
        started = false;
    }

    void dump( DatabaseContext ctx )
    {
        if ( !started )
        {
            return;
        }

        var namedDatabaseId = ctx.database().getNamedDatabaseId();
        try
        {
            var dbLayout = ctx.databaseFacade().databaseLayout();
            var databaseDirectory = dbLayout.databaseDirectory();
            var txDirectory = dbLayout.getTransactionLogsDirectory();
            var lockFile = dbLayout.databaseLockFile();
            var dumper = new Dumper();
            Predicate<Path> isLockFile = path -> Objects.equals( path.getFileName().toString(), lockFile.getFileName().toString() );
            var out = databaseDumpLocation( namedDatabaseId );
            dumper.dump( databaseDirectory, txDirectory, out, selectCompressionFormat(), isLockFile );
        }
        catch ( IOException e )
        {
            throw new DatabaseManagementException( format( "An error occurred! Unable to dump database with name `%s` whilst dropping it.",
                                                           namedDatabaseId.name() ), e );
        }
    }

    private Path databaseDumpLocation( NamedDatabaseId databaseId )
    {
        var shortDatabaseId = new Id( databaseId.databaseId().uuid() ).toString();
        var epochSeconds = Instant.now( clock ).getEpochSecond();
        var dumpDirName = String.format( "%s-%s-%s.dump", databaseId.name(), shortDatabaseId, epochSeconds );
        return dumpsRoot.resolve( dumpDirName );
    }
}
