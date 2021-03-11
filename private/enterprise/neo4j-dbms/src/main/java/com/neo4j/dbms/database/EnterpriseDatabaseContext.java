/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.dbms.database;

import org.neo4j.dbms.database.DatabaseContext;

public interface EnterpriseDatabaseContext extends DatabaseContext
{
    EnterpriseDatabase enterpriseDatabase();
}
