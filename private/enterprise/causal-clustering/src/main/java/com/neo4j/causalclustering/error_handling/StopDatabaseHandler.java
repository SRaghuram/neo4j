/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.error_handling;

import com.neo4j.dbms.ClusterInternalDbmsOperator;

import org.neo4j.kernel.database.Database;

public class StopDatabaseHandler implements DatabasePanicEventHandler
{
    private final Database db;
    private final ClusterInternalDbmsOperator internalOperator;

    StopDatabaseHandler( Database db, ClusterInternalDbmsOperator internalOperator )
    {
        this.db = db;
        this.internalOperator = internalOperator;
    }

    @Override
    public void onPanic( Throwable cause )
    {
        internalOperator.stopOnPanic( db.getDatabaseId(), cause );
    }
}
