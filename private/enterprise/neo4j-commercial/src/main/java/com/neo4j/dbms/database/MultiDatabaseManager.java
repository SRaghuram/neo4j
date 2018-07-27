/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.dbms.database;

import java.util.Collection;
import java.util.Optional;

import org.neo4j.dbms.database.DatabaseManager;
import org.neo4j.graphdb.facade.spi.ClassicCoreSPI;
import org.neo4j.graphdb.factory.module.DataSourceModule;
import org.neo4j.graphdb.factory.module.EditionModule;
import org.neo4j.graphdb.factory.module.PlatformModule;
import org.neo4j.helpers.Exceptions;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.kernel.impl.proc.Procedures;
import org.neo4j.kernel.impl.util.CopyOnWriteHashMap;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.logging.Logger;

public class MultiDatabaseManager extends LifecycleAdapter implements DatabaseManager
{
    private final CopyOnWriteHashMap<String, GraphDatabaseFacade> databaseMap = new CopyOnWriteHashMap<>();
    private final PlatformModule platform;
    private final EditionModule edition;
    private final Procedures procedures;
    private final Logger msgLog;
    private final GraphDatabaseFacade graphDatabaseFacade;

    public MultiDatabaseManager( PlatformModule platform, EditionModule edition, Procedures procedures,
            Logger msgLog, GraphDatabaseFacade graphDatabaseFacade )
    {
        this.platform = platform;
        this.edition = edition;
        this.procedures = procedures;
        this.msgLog = msgLog;
        this.graphDatabaseFacade = graphDatabaseFacade;
    }

    @Override
    public GraphDatabaseFacade createDatabase( String name )
    {
        GraphDatabaseFacade facade = DatabaseManager.DEFAULT_DATABASE_NAME.equals( name ) ? graphDatabaseFacade : new GraphDatabaseFacade();
        DataSourceModule dataSource = new DataSourceModule( name, platform, edition, procedures, facade );
        ClassicCoreSPI spi = new ClassicCoreSPI( platform, dataSource, msgLog, edition.coreAPIAvailabilityGuard );
        facade.init( spi, dataSource.threadToTransactionBridge, platform.config, dataSource.tokenHolders );
        databaseMap.put( name, facade );
        return facade;
    }

    @Override
    public Optional<GraphDatabaseFacade> getDatabaseFacade( String name )
    {
        return Optional.ofNullable( databaseMap.get( name ) );
    }

    @Override
    public void shutdownDatabase( String name )
    {
        GraphDatabaseFacade databaseFacade = databaseMap.remove( name );
        if ( databaseFacade != null )
        {
            databaseFacade.shutdown();
        }
    }

    @Override
    public void stop() throws Throwable
    {
        Collection<GraphDatabaseFacade> databaseFacades = databaseMap.values();
        Throwable stopException = null;
        for ( GraphDatabaseFacade databaseFacade : databaseFacades )
        {
            try
            {
                databaseFacade.shutdown();
            }
            catch ( Throwable t )
            {
                stopException = Exceptions.chain( stopException, t );
            }
        }
        databaseMap.clear();
        if ( stopException != null )
        {
            throw stopException;
        }
    }
}
