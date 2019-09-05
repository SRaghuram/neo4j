/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.procedure.commercial.builtin;

import com.neo4j.test.extension.CommercialDbmsExtension;

import org.neo4j.kernel.internal.GraphDatabaseAPI;

import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;

@CommercialDbmsExtension
class BuiltInEnterpriseProcedures extends SystemBuiltInEnterpriseProcedures
{
    // This class is to make sure we get the same result for listProcedures on user database

    @Override
    public GraphDatabaseAPI getGraphDatabaseAPI()
    {
        return (GraphDatabaseAPI)databaseManagementService.database( DEFAULT_DATABASE_NAME );
    }
}
