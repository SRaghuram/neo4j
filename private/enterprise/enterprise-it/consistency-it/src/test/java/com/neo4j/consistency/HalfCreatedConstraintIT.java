/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.consistency;

import com.neo4j.test.TestEnterpriseDatabaseManagementServiceBuilder;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.util.concurrent.TimeUnit;

import org.neo4j.common.DependencyResolver;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.consistency.ConsistencyCheckService;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.exceptions.KernelException;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.schema.LabelSchemaDescriptor;
import org.neo4j.internal.schema.SchemaDescriptor;
import org.neo4j.internal.schema.SchemaRule;
import org.neo4j.internal.schema.constraints.ConstraintDescriptorFactory;
import org.neo4j.internal.schema.constraints.UniquenessConstraintDescriptor;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.Neo4jLayoutExtension;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.internal.helpers.progress.ProgressMonitorFactory.NONE;

@Neo4jLayoutExtension
class HalfCreatedConstraintIT
{
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
        ConsistencyCheckService.Result checkResult =
                service.runFullConsistencyCheck( databaseLayout, Config.defaults(), NONE, NullLogProvider.getInstance(), false );
        assertTrue( checkResult.isSuccessful(), Files.readString( checkResult.reportFile().toPath() ) );
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
        assertThat( exception.getMessage(), containsString(
                    "Index IndexDefinition[label:MARKER on:property] " +
                            "(Index( 1, 'Uniqueness constraint on :Label (prop)', UNIQUE BTREE, :label[0](property[0]), native-btree-1.0 )) " +
                            "entered a FAILED state. Please see database logs.: Cause of failure:" ) );
    }

    private static void addIndex( GraphDatabaseService database ) throws KernelException
    {
        try ( Transaction transaction = database.beginTx() )
        {
            DependencyResolver resolver = ((GraphDatabaseAPI) database).getDependencyResolver();
            KernelTransaction kernelTransaction = ((InternalTransaction) transaction).kernelTransaction();
            LabelSchemaDescriptor schema = SchemaDescriptor.forLabel( 0, 0 );
            UniquenessConstraintDescriptor constraint = ConstraintDescriptorFactory.uniqueForSchema( schema );
            constraint = constraint.withName( SchemaRule.generateName( constraint, new String[]{"Label"}, new String[]{"prop"} ) );
            Config config = resolver.resolveDependency( Config.class );
            kernelTransaction.indexUniqueCreate( constraint, config.get( GraphDatabaseSettings.default_schema_provider ) );
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
