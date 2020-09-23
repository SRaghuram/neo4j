/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.consistency;

import com.neo4j.test.TestEnterpriseDatabaseManagementServiceBuilder;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;

import org.neo4j.configuration.Config;
import org.neo4j.consistency.ConsistencyCheckService;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.exceptions.KernelException;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.schema.IndexPrototype;
import org.neo4j.internal.schema.LabelSchemaDescriptor;
import org.neo4j.internal.schema.SchemaDescriptor;
import org.neo4j.internal.schema.SchemaRule;
import org.neo4j.internal.schema.constraints.ConstraintDescriptorFactory;
import org.neo4j.internal.schema.constraints.UniquenessConstraintDescriptor;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.kernel.impl.index.schema.GenericNativeIndexProvider;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.Neo4jLayoutExtension;
import org.neo4j.test.rule.TestDirectory;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.configuration.GraphDatabaseSettings.logs_directory;
import static org.neo4j.internal.helpers.progress.ProgressMonitorFactory.NONE;

@Neo4jLayoutExtension
class HalfCreatedConstraintIT
{
    @Inject
    private TestDirectory testDirectory;
    @Inject
    private DatabaseLayout databaseLayout;

    @Test
    void uniqueIndexWithoutOwningConstraintIsIgnoredDuringCheck() throws Exception
    {

        Label marker = Label.label( "MARKER" );
        String property = "property";

        DatabaseManagementService managementService = new TestEnterpriseDatabaseManagementServiceBuilder( databaseLayout )
                .build();
        GraphDatabaseService database = managementService.database( DEFAULT_DATABASE_NAME );
        try
        {
            createNodes( marker, property, database );
            addIndex( database );
            waitForIndexPopulationFailure( database );
        }
        finally
        {
            managementService.shutdown();
        }

        ConsistencyCheckService service = new ConsistencyCheckService();
        Path logs = testDirectory.cleanDirectory( "logs" );
        Config config = Config.defaults( logs_directory, logs );
        ConsistencyCheckService.Result checkResult =
                service.runFullConsistencyCheck( databaseLayout, config, NONE, NullLogProvider.getInstance(), false );
        assertTrue( checkResult.isSuccessful(), Files.readString( checkResult.reportFile() ) );
    }

    private static void waitForIndexPopulationFailure( GraphDatabaseService database )
    {
        IllegalStateException exception = assertThrows( IllegalStateException.class, () ->
        {
            try ( Transaction tx = database.beginTx() )
            {
                tx.schema().awaitIndexesOnline( 10, TimeUnit.MINUTES );
            }
        } );
        assertThat( exception.getMessage() ).contains(
                "Index IndexDefinition[label:MARKER on:property] (Index( id=1, name='constraint_952591e6', type='UNIQUE BTREE', schema=(:MARKER " +
                        "{property}), indexProvider='native-btree-1.0' )) entered a FAILED state. Please see database logs.: Cause of failure:" );
    }

    private static void addIndex( GraphDatabaseService database ) throws KernelException
    {
        try ( Transaction transaction = database.beginTx() )
        {
            KernelTransaction kernelTransaction = ((InternalTransaction) transaction).kernelTransaction();
            LabelSchemaDescriptor schema = SchemaDescriptor.forLabel( 0, 0 );
            UniquenessConstraintDescriptor constraint = ConstraintDescriptorFactory.uniqueForSchema( schema );
            constraint = constraint.withName( SchemaRule.generateName( constraint, new String[]{"Label"}, new String[]{"prop"} ) );
            IndexPrototype prototype = IndexPrototype.uniqueForSchema( schema )
                    .withName( constraint.getName() )
                    .withIndexProvider( GenericNativeIndexProvider.DESCRIPTOR );
            kernelTransaction.indexUniqueCreate( prototype );
            transaction.commit();
        }
    }

    private static void createNodes( Label marker, String property, GraphDatabaseService database )
    {
        try ( Transaction transaction = database.beginTx() )
        {
            for ( int i = 0; i < 10; i++ )
            {
                Node node = transaction.createNode( marker );
                node.setProperty( property, "a" );
            }
            transaction.commit();
        }
    }
}
