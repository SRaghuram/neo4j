/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.commandline.dbms;

import com.neo4j.causalclustering.core.state.ClusterStateLayout;

import java.nio.file.Path;

import org.neo4j.cli.ExecutionContext;
import org.neo4j.commandline.dbms.LoadCommand;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.dbms.archive.Loader;
import org.neo4j.io.fs.FileSystemAbstraction;

import static java.lang.String.format;

public class EnterpriseLoadCommand extends LoadCommand
{
    private final FileSystemAbstraction fs;

    public EnterpriseLoadCommand( ExecutionContext ctx, Loader loader )
    {
        super( ctx, loader );
        this.fs = ctx.fs();
    }

    @Override
    protected void loadDump()
    {
        Config config = buildConfig();
        Path raftGroupDirectory = getRaftGroupDirectory( database.name(), config );

        if ( fs.fileExists( raftGroupDirectory ) )
        {
            throw new IllegalArgumentException( format(
                    "Database with name [%s] already exists locally. " +
                    "Please run `DROP DATABASE %s` against the system database. " +
                    "If the database already is dropped, then you need to unbind the local instance using `neo4j-admin unbind`. " +
                    "Note that unbind requires stopping the instance, and affects all databases.",
                    database.name(), database.name() ) );
        }
        super.loadDump();
    }

    private Path getRaftGroupDirectory( String databaseName, Config config )
    {
        return ClusterStateLayout.of( config.get( GraphDatabaseSettings.data_directory ) ).raftGroupDir( databaseName );
    }
}
