/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.dbms.database;

import org.neo4j.dbms.database.StandaloneDatabaseContext;
import org.neo4j.kernel.database.Database;

public class StandaloneEnterpriseDatabaseContext extends StandaloneDatabaseContext implements EnterpriseDatabaseContext
{
    private final EnterpriseDatabase enterpriseDatabase;

    public StandaloneEnterpriseDatabaseContext( Database database, EnterpriseDatabase enterpriseDatabase )
    {
        super( database );
        this.enterpriseDatabase = enterpriseDatabase;
    }

    @Override
    public EnterpriseDatabase enterpriseDatabase()
    {
        return enterpriseDatabase;
    }
}
