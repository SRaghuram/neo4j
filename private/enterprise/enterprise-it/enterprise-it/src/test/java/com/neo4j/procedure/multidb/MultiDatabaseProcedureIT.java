/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.procedure.multidb;

import com.neo4j.test.TestEnterpriseDatabaseManagementServiceBuilder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.junit.jupiter.api.parallel.Resources;

import java.util.Set;

import org.neo4j.dbms.api.DatabaseExistsException;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.SuppressOutputExtension;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static java.util.stream.Collectors.toSet;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.internal.helpers.collection.Iterables.stream;
import static org.neo4j.internal.helpers.collection.Iterators.asSet;

@TestDirectoryExtension
@ExtendWith( SuppressOutputExtension.class )
@ResourceLock( Resources.SYSTEM_OUT )
class MultiDatabaseProcedureIT
{
    private static final Label MAPPER_LABEL = Label.label( "mapper" );

    @Inject
    private TestDirectory testDirectory;

    private DatabaseManagementService managementService;

    @BeforeEach
    void setUp()
    {
        managementService = new TestEnterpriseDatabaseManagementServiceBuilder( testDirectory.homePath() ).build();
    }

    @AfterEach
    void tearDown()
    {
        managementService.shutdown();
    }

    @Test
    void proceduresOperatesInCorrectDatabaseScope() throws DatabaseExistsException
    {
        String firstName = "first";
        String secondName = "second";

        managementService.createDatabase( firstName );
        managementService.createDatabase( secondName );

        GraphDatabaseFacade firstFacade = (GraphDatabaseFacade) managementService.database( firstName );
        GraphDatabaseFacade secondFacade = (GraphDatabaseFacade) managementService.database( secondName );

        createLabel( firstFacade, firstName );
        createLabel( secondFacade, secondName );

        Set<String> firstLabelNames = getAllLabelNames( firstFacade );
        Set<String> secondLabelNames = getAllLabelNames( secondFacade );

        assertEquals( firstLabelNames, asSet( firstName ) );
        assertEquals( secondLabelNames, asSet( secondName ) );
    }

    private Set<String> getAllLabelNames( GraphDatabaseFacade facade )
    {
        try ( Transaction transaction = facade.beginTx() )
        {
            return stream( transaction.getAllLabels() ).map( Label::name ).collect( toSet() );
        }
    }

    private void createLabel( GraphDatabaseFacade facade, String label )
    {
        try ( Transaction transaction = facade.beginTx() )
        {
            transaction.execute( "call db.createLabel(\"" + label + "\")" ).close();
            transaction.commit();
        }
    }
}
