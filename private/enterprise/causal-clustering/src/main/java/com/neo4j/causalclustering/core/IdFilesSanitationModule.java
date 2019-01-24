/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core;

import java.util.Optional;
import java.util.function.Supplier;

import org.neo4j.causalclustering.common.IdFilesDeleter;
import org.neo4j.dbms.database.DatabaseContext;
import org.neo4j.dbms.database.DatabaseManager;
import org.neo4j.io.fs.FileSystemAbstraction;
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
    private final CoreStartupState coreStartupState;

    IdFilesSanitationModule( CoreStartupState coreStartupState, Supplier<DatabaseManager> databaseManagerSupplier, FileSystemAbstraction fileSystem,
            LogProvider logProvider )
    {
        this.coreStartupState = coreStartupState;
        this.databaseManagerSupplier = databaseManagerSupplier;
        this.fileSystem = fileSystem;
        this.log = logProvider.getLog( getClass() );
    }

    @Override
    public void start() throws Throwable
    {
        if ( !coreStartupState.wasUnboundOnStartup() )
        {
            return;
        }

        DatabaseManager databaseManager = databaseManagerSupplier.get();

        for ( String databaseName : databaseManager.listDatabases() )
        {
            Optional<GraphDatabaseFacade> database = databaseManager
                    .getDatabaseContext( databaseName )
                    .map( DatabaseContext::getDatabaseFacade );

            database.ifPresent( db ->
            {
                if ( IdFilesDeleter.deleteIdFiles( db.databaseLayout(), fileSystem ) )
                {
                    log.info( String.format( "ID-files deleted for %s", databaseName ) );
                }
            } );
        }
    }
}
