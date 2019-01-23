/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.kernel.impl.procesure;

import com.neo4j.test.TestCommercialGraphDatabaseFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.Set;
import java.util.stream.Stream;

import org.neo4j.common.DependencyResolver;
import org.neo4j.dbms.database.DatabaseContext;
import org.neo4j.dbms.database.DatabaseManager;
import org.neo4j.exceptions.KernelException;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.impl.core.NodeProxy;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.kernel.impl.proc.GlobalProcedures;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Procedure;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.SuppressOutputExtension;
import org.neo4j.test.extension.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static java.util.stream.Collectors.toSet;
import static java.util.stream.Stream.of;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.neo4j.helpers.collection.Iterators.asSet;
import static org.neo4j.helpers.collection.Iterators.single;

@ExtendWith( {TestDirectoryExtension.class, SuppressOutputExtension.class} )
class MultiDatabaseProcedureIT
{
    private static final Label MAPPER_LABEL = Label.label( "mapper" );

    @Inject
    private TestDirectory testDirectory;

    private GraphDatabaseAPI database;
    private DatabaseManager databaseManager;

    @BeforeEach
    void setUp()
    {
        database = (GraphDatabaseAPI) new TestCommercialGraphDatabaseFactory().newEmbeddedDatabase( testDirectory.databaseDir() );
        databaseManager = getDatabaseManager();
    }

    @AfterEach
    void tearDown()
    {
        database.shutdown();
    }

    @Test
    void proceduresOperatesIncorrectDatabaseScope()
    {
        String firstName = "first";
        String secondName = "second";

        DatabaseContext firstDatabase = databaseManager.createDatabase( firstName );
        DatabaseContext secondDatabase = databaseManager.createDatabase( secondName );

        GraphDatabaseFacade firstFacade = firstDatabase.getDatabaseFacade();
        GraphDatabaseFacade secondFacade = secondDatabase.getDatabaseFacade();

        createLabel( firstFacade, firstName );
        createLabel( secondFacade, secondName );

        Set<String> firstLabelNames = getAllLabelNames( firstFacade );
        Set<String> secondLabelNames = getAllLabelNames( secondFacade );

        assertEquals( firstLabelNames, asSet( firstName ) );
        assertEquals( secondLabelNames, asSet( secondName ) );
    }

    @Test
    void proceduresUseDatabaseLocalValueMapper() throws KernelException
    {
        GlobalProcedures globalProcedures = database.getDependencyResolver().resolveDependency( GlobalProcedures.class );
        globalProcedures.registerProcedure( MappingProcedure.class );

        String firstName = "mapperFirst";
        String secondName = "mapperSecond";

        DatabaseContext firstDatabase = databaseManager.createDatabase( firstName );
        DatabaseContext secondDatabase = databaseManager.createDatabase( secondName );

        GraphDatabaseFacade firstFacade = firstDatabase.getDatabaseFacade();
        GraphDatabaseFacade secondFacade = secondDatabase.getDatabaseFacade();

        createMarkerNode( firstFacade );
        createMarkerNode( secondFacade );

        checkUsedFacadeForProxies( firstFacade );
        checkUsedFacadeForProxies( secondFacade );
    }

    private void checkUsedFacadeForProxies( GraphDatabaseFacade facade )
    {
        try ( Transaction ignored = facade.beginTx() )
        {
            try ( Result result = facade.execute( "call multidb.valueMapper" ) )
            {
                NodeProxy node = (NodeProxy) result.next().get( "node" );
                assertSame( facade, node.getGraphDatabase() );
            }
        }
    }

    private void createMarkerNode( GraphDatabaseFacade facade )
    {
        try ( Transaction tx = facade.beginTx() )
        {
            facade.createNode( MAPPER_LABEL );
            tx.success();
        }
    }

    private Set<String> getAllLabelNames( GraphDatabaseFacade facade )
    {
        try ( Transaction ignored = facade.beginTx() )
        {
            return facade.getAllLabels().stream().map( Label::name ).collect( toSet() );
        }
    }

    private void createLabel( GraphDatabaseFacade facade, String label )
    {
        try ( Transaction transaction = facade.beginTx() )
        {
            facade.execute( "call db.createLabel(\"" + label + "\")" ).close();
            transaction.success();
        }
    }

    private DatabaseManager getDatabaseManager()
    {
        return database.getDependencyResolver().resolveDependency( DatabaseManager.class );
    }

    @SuppressWarnings( "WeakerAccess" )
    public static class MappingProcedure
    {
        @Context
        public GraphDatabaseAPI databaseAPI;

        @Context
        public DependencyResolver resolver;

        @Procedure( name = "multidb.valueMapper" )
        public Stream<CheckResult> valueMapperCheck()
        {
            assertSame( databaseAPI.getDependencyResolver(), resolver );
            return of( single( databaseAPI.findNodes( MAPPER_LABEL ) ) ).map( CheckResult::new );
        }

        public static class CheckResult
        {
            public Node node;

            CheckResult( Node node )
            {
                this.node = node;
            }
        }
    }
}
