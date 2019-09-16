/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.procedure.routing;

import com.neo4j.test.TestEnterpriseDatabaseManagementServiceBuilder;

import java.io.File;

import org.neo4j.dbms.api.DatabaseManagementServiceBuilder;

class EnterpriseSingleInstanceRoutingProcedureIT extends CommunitySingleInstanceRoutingProcedureIT
{
    @Override
    protected DatabaseManagementServiceBuilder newGraphDatabaseFactory( File databaseRootDir )
    {
        return new TestEnterpriseDatabaseManagementServiceBuilder( databaseRootDir );
    }
}
