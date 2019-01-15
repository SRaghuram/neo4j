/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.core;

import java.io.File;
import java.io.IOException;
import java.util.Optional;
import java.util.function.Supplier;

import org.neo4j.dbms.database.DatabaseManager;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

/**
 * Removes the ID-files if the instance was unbound. Reason being that an unbound instance likely
 * has a store which has been tampered with in some sense, e.g. copied around, restored from a backup,
 * dumped/loaded etc and we cannot trust that the ID-files have not been copied around. ID-files are
 * private per instance and shall not be copied around.
 *
 * This is thus a defensive strategy to avoid corruption caused by various operational actions.
 */
public class IdFilesSanitationModule extends LifecycleAdapter
{
    private final Supplier<DatabaseManager> databaseManagerSupplier;
    private final FileSystemAbstraction fileSystem;
    private final Log log;
    private final boolean unboundOnCreation;

    IdFilesSanitationModule( boolean wasUnboundOnCreation, Supplier<DatabaseManager> databaseManagerSupplier, FileSystemAbstraction fileSystem,
            LogProvider logProvider )
    {
        this.unboundOnCreation = wasUnboundOnCreation;
        this.databaseManagerSupplier = databaseManagerSupplier;
        this.fileSystem = fileSystem;
        this.log = logProvider.getLog( getClass() );
    }

    @Override
    public void start() throws Throwable
    {
        if ( !unboundOnCreation )
        {
            return;
        }

        DatabaseManager databaseManager = databaseManagerSupplier.get();

        for ( String databaseName : databaseManager.listDatabases() )
        {
            Optional<GraphDatabaseFacade> database = databaseManager.getDatabaseFacade( databaseName );
            database.ifPresent( db -> deleteIdFiles( databaseName, db ) );
        }
    }

    private void deleteIdFiles( String databaseName, GraphDatabaseFacade db )
    {
        if ( deleteIdFiles( db.databaseLayout() ) )
        {
            log.info( String.format( "ID-files deleted for %s", databaseName ) );
        }
    }

    private boolean deleteIdFiles( DatabaseLayout databaseLayout )
    {
        if ( !fileSystem.fileExists( databaseLayout.databaseDirectory() ) )
        {
            return false;
        }

        boolean anyIdFilesDeleted = false;
        for ( File idFile : databaseLayout.idFiles() )
        {
            try
            {
                if ( fileSystem.fileExists( idFile ) )
                {
                    fileSystem.deleteFileOrThrow( idFile );
                }
                anyIdFilesDeleted = true;
            }
            catch ( IOException e )
            {
                throw new RuntimeException( "Could not delete ID-file", e );
            }
        }

        return anyIdFilesDeleted;
    }
}
