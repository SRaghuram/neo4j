/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.dbms.database;

import org.neo4j.collection.Dependencies;
import org.neo4j.configuration.Config;
import org.neo4j.dbms.database.DatabaseContext;
import org.neo4j.dbms.database.DatabaseOperationCounts;
import org.neo4j.dbms.database.StandaloneDatabaseContext;
import org.neo4j.graphdb.factory.module.GlobalModule;
import org.neo4j.kernel.database.Database;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.kernel.internal.event.GlobalTransactionEventListeners;
import org.neo4j.kernel.monitoring.DatabaseEventListeners;
import org.neo4j.logging.NullLog;
import org.neo4j.logging.internal.NullLogService;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.storageengine.api.TransactionIdStore;
import org.neo4j.test.scheduler.CallingThreadJobScheduler;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

//TODO: Merge this and StubClusteredDatabasemanager into a single class heirarchy
public class StubMultiDatabaseManager extends MultiDatabaseManager<DatabaseContext>
{
    public StubMultiDatabaseManager()
    {
        this( new CallingThreadJobScheduler() );
    }

    public StubMultiDatabaseManager( JobScheduler jobScheduler )
    {
        super( mockGlobalModule( jobScheduler ), null, true );
    }

    @Override
    protected DatabaseContext createDatabaseContext( NamedDatabaseId namedDatabaseId )
    {
        return mockDatabaseContext( namedDatabaseId );
    }

    public GlobalModule globalModule()
    {
        return globalModule;
    }

    private static DatabaseContext mockDatabaseContext( NamedDatabaseId namedDatabaseId )
    {
        var facade = mock( GraphDatabaseFacade.class );
        Dependencies deps = new Dependencies();
        deps.satisfyDependencies( mock( TransactionIdStore.class ) );
        when( facade.getDependencyResolver() ).thenReturn( deps );
        Database db = mock( Database.class );
        when( db.getNamedDatabaseId() ).thenReturn( namedDatabaseId );
        when( db.getDatabaseFacade() ).thenReturn( facade );
        return spy( new StandaloneDatabaseContext( db ) );
    }

    static GlobalModule mockGlobalModule( JobScheduler jobScheduler )
    {
        Dependencies dependencies = new Dependencies();
        GlobalModule module = mock( GlobalModule.class );
        when( module.getGlobalDependencies() ).thenReturn( dependencies );
        when( module.getDatabaseEventListeners() ).thenReturn( new DatabaseEventListeners( NullLog.getInstance() ) );
        when( module.getGlobalConfig() ).thenReturn( Config.defaults() );
        when( module.getLogService() ).thenReturn( NullLogService.getInstance() );
        when( module.getExternalDependencyResolver() ).thenReturn( new Dependencies() );
        when( module.getJobScheduler() ).thenReturn( jobScheduler );
        when( module.getTransactionEventListeners() ).thenReturn( new GlobalTransactionEventListeners() );
        dependencies.satisfyDependency( new DatabaseOperationCounts.Counter() );
        return module;
    }
}
