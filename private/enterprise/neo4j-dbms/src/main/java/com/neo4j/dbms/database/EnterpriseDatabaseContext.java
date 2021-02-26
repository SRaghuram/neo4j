/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.dbms.database;

import org.neo4j.dbms.database.StandaloneDatabaseContext;
import org.neo4j.kernel.database.Database;

public class EnterpriseDatabaseContext extends StandaloneDatabaseContext implements CompositeDatabaseContext
{
    private final CompositeDatabase enterpriseDatabase;

    public EnterpriseDatabaseContext( Database database, CompositeDatabase enterpriseDatabase )
    {
        super( database );
        this.enterpriseDatabase = enterpriseDatabase;
    }

    @Override
    public CompositeDatabase compositeDatabase()
    {
        return enterpriseDatabase;
    }
}
