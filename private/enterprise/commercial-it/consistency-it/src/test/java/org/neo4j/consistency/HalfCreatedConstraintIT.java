/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.consistency;

import com.neo4j.test.TestCommercialGraphDatabaseFactory;
import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.io.PrintStream;
import java.nio.file.Files;
import java.util.concurrent.TimeUnit;

import org.neo4j.common.DependencyResolver;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.kernel.api.exceptions.schema.SchemaKernelException;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.schema.LabelSchemaDescriptor;
import org.neo4j.kernel.api.schema.SchemaDescriptorFactory;
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.rule.TestDirectory;

import static org.hamcrest.CoreMatchers.containsString;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class HalfCreatedConstraintIT
{
    @Rule
    public final TestDirectory testDirectory = TestDirectory.testDirectory();

    @Test
    public void uniqueIndexWithoutOwningConstraintIsIgnoredDuringCheck() throws Exception
    {
        File databaseDir = testDirectory.databaseDir();
        Label marker = Label.label( "MARKER" );
        String property = "property";

        GraphDatabaseService database = new TestCommercialGraphDatabaseFactory().newEmbeddedDatabase( databaseDir );
        try
        {
            createNodes( marker, property, database );
            addIndex( database );
            waitForIndexPopulationFailure( database );
        }
        finally
        {
            database.shutdown();
        }

        ConsistencyCheckService.Result checkResult = ConsistencyCheckTool.runConsistencyCheckTool( new String[]{databaseDir.getAbsolutePath()},
                emptyPrintStream(), emptyPrintStream() );
        assertTrue( String.join( System.lineSeparator(), Files.readAllLines( checkResult.reportFile().toPath() ) ), checkResult.isSuccessful() );
    }

    private static void waitForIndexPopulationFailure( GraphDatabaseService database )
    {
        try ( Transaction ignored = database.beginTx() )
        {
            database.schema().awaitIndexesOnline( 10, TimeUnit.MINUTES );
            fail( "Unique index population should fail." );
        }
        catch ( IllegalStateException e )
        {
            assertThat( e.getMessage(), containsString( "Index entered a FAILED state. Please see database logs.: Cause of failure:" ) );
        }
    }

    private static void addIndex( GraphDatabaseService database ) throws SchemaKernelException
    {
        try ( Transaction transaction = database.beginTx() )
        {
            DependencyResolver resolver = ((GraphDatabaseAPI) database).getDependencyResolver();
            ThreadToStatementContextBridge statementBridge = resolver.provideDependency( ThreadToStatementContextBridge.class ).get();
            KernelTransaction kernelTransaction = statementBridge.getKernelTransactionBoundToThisThread( true );
            LabelSchemaDescriptor descriptor = SchemaDescriptorFactory.forLabel( 0, 0 );
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

    private static PrintStream emptyPrintStream()
    {
        return new PrintStream( org.neo4j.io.NullOutputStream.NULL_OUTPUT_STREAM );
    }
}
