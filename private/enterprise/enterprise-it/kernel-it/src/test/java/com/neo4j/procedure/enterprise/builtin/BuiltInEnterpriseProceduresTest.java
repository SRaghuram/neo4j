/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.procedure.enterprise.builtin;

import com.neo4j.test.extension.EnterpriseDbmsExtension;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.List;

import org.neo4j.collection.RawIterator;
import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.internal.kernel.api.procs.ProcedureCallContext;
import org.neo4j.internal.kernel.api.procs.QualifiedName;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.security.AnonymousContext;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.procedure.builtin.BuiltInDbmsProcedures;
import org.neo4j.values.AnyValue;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.internal.helpers.collection.Iterators.asList;
import static org.neo4j.internal.kernel.api.procs.ProcedureSignature.procedureName;
import static org.neo4j.kernel.api.KernelTransaction.Type.IMPLICIT;
import static org.neo4j.values.storable.Values.stringValue;

@EnterpriseDbmsExtension
class BuiltInEnterpriseProceduresTest extends SystemBuiltInEnterpriseProceduresTest
{
    @Override
    public GraphDatabaseAPI getGraphDatabaseAPI()
    {
        return (GraphDatabaseAPI)databaseManagementService.database( DEFAULT_DATABASE_NAME );
    }

    @Test
    void checkThatEnterpriseAndCommunityHaveSameResultColumnsForListProcedures()
    {
        checkClassesHaveSameFinalFields( BuiltInDbmsProcedures.ProcedureResult.class, EnterpriseBuiltInDbmsProcedures.ProcedureResult.class );
    }

    @Test
    void checkThatEnterpriseAndCommunityHaveSameResultColumnsForListFunctions()
    {
        checkClassesHaveSameFinalFields( BuiltInDbmsProcedures.FunctionResult.class, EnterpriseBuiltInDbmsProcedures.FunctionResult.class );
    }

    @Test
    void listClientConfig() throws Exception
    {
        QualifiedName procedureName = procedureName( "dbms", "clientConfig" );
        GraphDatabaseAPI system = getGraphDatabaseAPI();
        KernelTransaction transaction = system.beginTransaction( IMPLICIT, AnonymousContext.read() ).kernelTransaction();
        int procedureId = transaction.procedures().procedureGet( procedureName ).id();
        RawIterator<AnyValue[],ProcedureException> callResult =
                transaction.procedures().procedureCallRead( procedureId, new AnyValue[]{}, ProcedureCallContext.EMPTY );
        List<AnyValue[]> config = asList( callResult );
        assertEquals( 7, config.size());

        assertEquals( config.get( 0 )[0], stringValue( "browser.allow_outgoing_connections" ));
        assertEquals( config.get( 1 )[0], stringValue( "browser.credential_timeout" ));
        assertEquals( config.get( 2 )[0], stringValue( "browser.post_connect_cmd" ));
        assertEquals( config.get( 3 )[0], stringValue( "browser.remote_content_hostname_whitelist" ));
        assertEquals( config.get( 4 )[0], stringValue( "browser.retain_connection_credentials" ));
        assertEquals( config.get( 5 )[0], stringValue( "dbms.default_database" ));
        assertEquals( config.get( 6 )[0], stringValue( "dbms.security.auth_enabled" ));
    }

    private static void checkClassesHaveSameFinalFields( Class<?> community, Class<?> enterprise )
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
