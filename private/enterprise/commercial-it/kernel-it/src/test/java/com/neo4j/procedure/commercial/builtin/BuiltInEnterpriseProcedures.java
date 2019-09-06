/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.procedure.commercial.builtin;

import com.neo4j.test.extension.CommercialDbmsExtension;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;

import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.procedure.builtin.BuiltInDbmsProcedures;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;

@CommercialDbmsExtension
class BuiltInEnterpriseProcedures extends SystemBuiltInEnterpriseProcedures
{
    @Override
    public GraphDatabaseAPI getGraphDatabaseAPI()
    {
        return (GraphDatabaseAPI)databaseManagementService.database( DEFAULT_DATABASE_NAME );
    }

    @Test
    public void checkThatEnterpriseAndCommunityHaveSameResultColumsForListProcedures()
    {
        checkClassesHaveSameFinalFields( BuiltInDbmsProcedures.ProcedureResult.class, EnterpriseBuiltInDbmsProcedures.ProcedureResult.class );
    }

    @Test
    public void checkThatEnterpriseAndCommunityHaveSameResultColumsForListFunctions()
    {
        checkClassesHaveSameFinalFields( BuiltInDbmsProcedures.FunctionResult.class, EnterpriseBuiltInDbmsProcedures.FunctionResult.class );
    }

    private void checkClassesHaveSameFinalFields( Class community, Class enterprise )
    {
        Field[] communityFields = community.getFields();
        Field[] enterpriseFields = enterprise.getFields();
        assertEquals( communityFields.length, enterpriseFields.length );

        for ( int i = 0; i < communityFields.length; i++ )
        {
            Field comField = communityFields[i];
            Field entField = enterpriseFields[i];

            assertTrue( Modifier.isFinal( comField.getModifiers() ) );
            assertTrue( Modifier.isFinal( entField.getModifiers() ) );
            assertEquals( comField.getName(), entField.getName() );
        }
    }
}
