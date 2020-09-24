/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.dbms.database;

import java.util.Optional;

import org.neo4j.exceptions.UnsatisfiedDependencyException;
import org.neo4j.io.fs.WritableChecksumChannel;
import org.neo4j.kernel.database.Database;
import org.neo4j.kernel.database.LogEntryWriterFactory;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryWriter;

public class DbmsLogEntryWriterProvider
{
    private DbmsLogEntryWriterProvider()
    {
        this( null );
    }

    private final MultiDatabaseManager<?> databaseManager;

    public DbmsLogEntryWriterProvider( MultiDatabaseManager<?> databaseManager )
    {
        this.databaseManager = databaseManager;
    }

    public LogEntryWriterFactory getEntryWriterFactory( NamedDatabaseId namedDatabaseId )
    {
        return Optional.ofNullable( namedDatabaseId )
                       .flatMap( databaseManager::getDatabaseContext )
                       .flatMap( ctx -> resolveEntryWriterFactoryOpt( ctx.database() ) )
                       .orElse( LogEntryWriterFactory.LATEST );
    }

    public static LogEntryWriterFactory resolveEntryWriterFactory( Database database )
    {
        return resolveEntryWriterFactoryOpt( database ).orElse( LogEntryWriterFactory.LATEST );
    }

    private static Optional<LogEntryWriterFactory> resolveEntryWriterFactoryOpt( Database database )
    {
        try
        {
            return Optional.ofNullable( database )
                           .map( db -> db.getDependencyResolver().resolveDependency( LogEntryWriterFactory.class ) );
        }
        catch ( UnsatisfiedDependencyException e )
        {
            return Optional.empty();
        }
    }

    public <T extends WritableChecksumChannel> LogEntryWriter<T> createEntryWriter( NamedDatabaseId namedDatabaseId, T channel )
    {
        return getEntryWriterFactory( namedDatabaseId ).createEntryWriter( channel );
    }

    public static DbmsLogEntryWriterProvider ALWAYS_LATEST = new DbmsLogEntryWriterProvider()
    {
        @Override
        public LogEntryWriterFactory getEntryWriterFactory( NamedDatabaseId namedDatabaseId )
        {
            return LogEntryWriterFactory.LATEST;
        }
    };
}