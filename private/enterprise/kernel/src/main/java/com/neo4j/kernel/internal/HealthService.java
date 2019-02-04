/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.kernel.internal;

import java.util.Optional;

import org.neo4j.dbms.database.DatabaseExistsException;
import org.neo4j.dbms.database.DatabaseNotFoundException;
import org.neo4j.monitoring.SingleDatabaseHealth;
import org.neo4j.logging.Log;
import org.neo4j.monitoring.DatabasePanicEventGenerator;

public interface HealthService
{
    SingleDatabaseHealth createDatabaseHealth( String databaseName, DatabasePanicEventGenerator dbpe, Log log ) throws DatabaseExistsException;

    void removeDatabaseHealth( String databaseName ) throws DatabaseNotFoundException;

    Optional<SingleDatabaseHealth> getDatabaseHealth( String databaseName );
}
