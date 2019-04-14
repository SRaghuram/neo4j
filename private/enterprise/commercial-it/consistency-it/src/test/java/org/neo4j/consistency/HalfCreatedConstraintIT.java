/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.consistency;

import com.neo4j.test.TestCommercialGraphDatabaseFactory;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.nio.file.Files;
import java.util.concurrent.TimeUnit;

import org.neo4j.common.DependencyResolver;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.dbms.database.DatabaseManagementService;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.kernel.api.exceptions.schema.SchemaKernelException;
import org.neo4j.internal.schema.DefaultLabelSchemaDescriptor;
import org.neo4j.internal.schema.SchemaDescriptorFactory;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.helpers.progress.ProgressMonitorFactory.NONE;

@ExtendWith( TestDirectoryExtension.class )
class HalfCreatedConstraintIT
{
    @Inject
    private TestDirectory testDirectory;

    @Test
    void uniqueIndexWithoutOwningConstraintIsIgnoredDuringCheck() throws Exception
    {
        DatabaseLayout databaseLayout = testDirectory.databaseLayout();
        Label marker = Label.label( "MARKER" );
        String property = "property";

        DatabaseManagementService managementService = new TestCommercialGraphDatabaseFactory()
                .newEmbeddedDatabaseBuilder( testDirectory.storeDir() ).newDatabaseManagementService();
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
            try ( Transaction ignored = database.beginTx() )
            {
                database.schema().awaitIndexesOnline( 10, TimeUnit.MINUTES );
            }
        } );
        assertThat( exception.getMessage(), containsString(
                    "Index IndexDefinition[label:MARKER on:property] (IndexRule[id=1, descriptor=Index( UNIQUE, :label[0](property[0]) ), " +
                            "provider={key=native-btree, version=1.0}, owner=null]) entered a FAILED state. Please see database logs.: Cause of failure:" ) );
    }

    private static void addIndex( GraphDatabaseService database ) throws SchemaKernelException
    {
        try ( Transaction transaction = database.beginTx() )
        {
            DependencyResolver resolver = ((GraphDatabaseAPI) database).getDependencyResolver();
            ThreadToStatementContextBridge statementBridge = resolver.provideDependency( ThreadToStatementContextBridge.class ).get();
            KernelTransaction kernelTransaction = statementBridge.getKernelTransactionBoundToThisThread( true );
            DefaultLabelSchemaDescriptor descriptor = SchemaDescriptorFactory.forLabel( 0, 0 );
            Config config = resolver.resolveDependency( Config.class );
            kernelTransaction.indexUniqueCreate( descriptor, config.get( GraphDatabaseSettings.default_schema_provider ) );
            transaction.success();
        }
    }

    private static void createNodes( Label marker, String property, GraphDatabaseService database )
    {
        try ( Transaction transaction = database.beginTx() )
        {
            for ( int i = 0; i < 10; i++ )
            {
                Node node = database.createNode( marker );
                node.setProperty( property, "a" );
            }
            transaction.success();
        }
    }
}
