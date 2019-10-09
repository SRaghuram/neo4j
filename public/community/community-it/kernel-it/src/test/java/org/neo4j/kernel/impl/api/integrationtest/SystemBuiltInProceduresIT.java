/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.impl.api.integrationtest;

import org.junit.jupiter.api.Test;

import java.util.List;

import org.neo4j.collection.RawIterator;
import org.neo4j.internal.kernel.api.TokenWrite;
import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.internal.kernel.api.procs.ProcedureCallContext;
import org.neo4j.internal.kernel.api.security.LoginContext;
import org.neo4j.internal.schema.LabelSchemaDescriptor;
import org.neo4j.internal.schema.SchemaDescriptor;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.security.AnonymousContext;
import org.neo4j.kernel.impl.util.DefaultValueMapper;
import org.neo4j.kernel.internal.Version;
import org.neo4j.values.AnyValue;
import org.neo4j.values.virtual.VirtualValues;

import static java.util.concurrent.TimeUnit.MINUTES;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;
import static org.neo4j.internal.helpers.collection.Iterators.asList;
import static org.neo4j.internal.kernel.api.procs.ProcedureSignature.procedureName;
import static org.neo4j.internal.kernel.api.security.LoginContext.AUTH_DISABLED;
import static org.neo4j.internal.schema.SchemaDescriptor.forLabel;
import static org.neo4j.kernel.api.ResourceManager.EMPTY_RESOURCE_MANAGER;
import static org.neo4j.values.storable.Values.stringValue;

class SystemBuiltInProceduresIT extends CommunityProcedureITBase
{
    @Override
    public String getDatabaseName()
    {
        // This makes sure that "db" is always system in this file.
        // It is not initialized with the security model and you should never try to change to a user db
        return SYSTEM_DATABASE_NAME;
    }

    @Test
    void listAllLabels() throws Throwable
    {
        // Given
        KernelTransaction transaction = newTransaction( AnonymousContext.writeToken() );
        long nodeId = transaction.dataWrite().nodeCreate();
        int labelId = transaction.tokenWrite().labelGetOrCreateForName( "MyLabel" );
        transaction.dataWrite().nodeAddLabel( nodeId, labelId );
        commit();

        try ( org.neo4j.graphdb.Transaction tx = db.beginTx() )
        {
            // When & Then
            assertFalse( tx.execute( "CALL db.labels" ).hasNext());
        }
    }

    @Test
    void listPropertyKeys() throws Throwable
    {
        // Given
        TokenWrite ops = tokenWriteInNewTransaction();
        ops.propertyKeyGetOrCreateForName( "MyProp" );
        commit();

        try ( org.neo4j.graphdb.Transaction tx = db.beginTx() )
        {
            // When & Then
            assertFalse( tx.execute( "CALL db.propertyKeys" ).hasNext());
        }
    }

    @Test
    void listRelationshipTypes() throws Throwable
    {
        // Given
        KernelTransaction transaction = newTransaction( AnonymousContext.writeToken() );
        int relType = transaction.tokenWrite().relationshipTypeGetOrCreateForName( "MyRelType" );
        long startNodeId = transaction.dataWrite().nodeCreate();
        long endNodeId = transaction.dataWrite().nodeCreate();
        transaction.dataWrite().relationshipCreate( startNodeId, relType, endNodeId );
        commit();

        try ( org.neo4j.graphdb.Transaction tx = db.beginTx() )
        {
            // When & Then
            assertFalse( tx.execute( "CALL db.relationshipTypes" ).hasNext());
        }
    }

    @Test
    void listAllComponentsShouldWork() throws Throwable
    {
        // its NOT a dummy procedure on system

        // When
        RawIterator<AnyValue[],ProcedureException> stream =
                procs().procedureCallRead( procs().procedureGet( procedureName( "dbms", "components" ) ).id(), new AnyValue[0],
                        ProcedureCallContext.EMPTY );

        // Then
        assertThat( asList( stream ), contains( equalTo( new AnyValue[]{stringValue( "Neo4j Kernel" ),
                VirtualValues.list( stringValue( Version.getNeo4jVersion() ) ), stringValue( "community" )} ) ) );

        commit();
    }

    @Test
    void listAllIndexes() throws Throwable
    {
        // Given
        KernelTransaction transaction = newTransaction( AUTH_DISABLED );
        int labelId1 = transaction.tokenWrite().labelGetOrCreateForName( "Person" );
        int labelId2 = transaction.tokenWrite().labelGetOrCreateForName( "Age" );
        int propertyKeyId1 = transaction.tokenWrite().propertyKeyGetOrCreateForName( "foo" );
        int propertyKeyId2 = transaction.tokenWrite().propertyKeyGetOrCreateForName( "bar" );
        LabelSchemaDescriptor personFooDescriptor = forLabel( labelId1, propertyKeyId1 );
        LabelSchemaDescriptor ageFooDescriptor = forLabel( labelId2, propertyKeyId1 );
        LabelSchemaDescriptor personFooBarDescriptor = forLabel( labelId1, propertyKeyId1, propertyKeyId2 );
        transaction.schemaWrite().indexCreate( personFooDescriptor, "person foo index" );
        transaction.schemaWrite().uniquePropertyConstraintCreate( ageFooDescriptor, "constraint name" );
        transaction.schemaWrite().indexCreate( personFooBarDescriptor, "person foo bar index" );
        commit();

        //let indexes come online
        try ( org.neo4j.graphdb.Transaction tx = db.beginTx() )
        {
            tx.schema().awaitIndexesOnline( 2, MINUTES );
            tx.commit();
        }

        try ( org.neo4j.graphdb.Transaction tx = db.beginTx() )
        {
            // When & Then
            assertFalse( tx.execute( "CALL db.indexes" ).hasNext());
        }
    }

    @Test
    void awaitIndexes() throws Throwable
    {
        // Given
        KernelTransaction transaction = newTransaction( AUTH_DISABLED );
        int labelId1 = transaction.tokenWrite().labelGetOrCreateForName( "Person" );
        int labelId2 = transaction.tokenWrite().labelGetOrCreateForName( "Age" );
        int propertyKeyId1 = transaction.tokenWrite().propertyKeyGetOrCreateForName( "foo" );
        int propertyKeyId2 = transaction.tokenWrite().propertyKeyGetOrCreateForName( "bar" );
        LabelSchemaDescriptor personFooDescriptor = forLabel( labelId1, propertyKeyId1 );
        LabelSchemaDescriptor ageFooDescriptor = forLabel( labelId2, propertyKeyId1 );
        LabelSchemaDescriptor personFooBarDescriptor = forLabel( labelId1, propertyKeyId1, propertyKeyId2 );
        transaction.schemaWrite().indexCreate( personFooDescriptor, "person foo index" );
        transaction.schemaWrite().uniquePropertyConstraintCreate( ageFooDescriptor, "constraint name" );
        transaction.schemaWrite().indexCreate( personFooBarDescriptor, "person foo bar index" );
        commit();

        try ( org.neo4j.graphdb.Transaction tx = db.beginTx() )
        {
            // When & Then
            // this will always be true because that procedure returns void BUT it proves that it runs on system
            assertFalse( tx.execute( "CALL db.awaitIndexes(10)" ).hasNext());
        }
    }

    @Test
    void awaitIndex() throws Throwable
    {
        // Given
        KernelTransaction transaction = newTransaction( AUTH_DISABLED );
        int labelId1 = transaction.tokenWrite().labelGetOrCreateForName( "Person" );
        int propertyKeyId1 = transaction.tokenWrite().propertyKeyGetOrCreateForName( "foo" );
        LabelSchemaDescriptor personFooDescriptor = forLabel( labelId1, propertyKeyId1 );
        transaction.schemaWrite().indexCreate( personFooDescriptor, "person foo index" );
        commit();

        try ( org.neo4j.graphdb.Transaction tx = db.beginTx() )
        {
            // When & Then
            // this will always be true because that procedure returns void BUT it proves that it runs on system
            assertFalse( tx.execute( "CALL db.awaitIndex(':Person(name)',10)" ).hasNext());
        }
    }

    @Test
    void listConstraints() throws Throwable
    {
        // Given
        KernelTransaction transaction = newTransaction( AUTH_DISABLED );
        transaction.schemaWrite().uniquePropertyConstraintCreate( SchemaDescriptor.forLabel( 1,1 ),"Test" );
        commit();

        try ( org.neo4j.graphdb.Transaction tx = db.beginTx() )
        {
            // When & Then
            assertFalse( tx.execute( "CALL db.constraints" ).hasNext());
        }
    }

    @Test
    void resampleIndex() throws Throwable
    {
        // Given
        KernelTransaction transaction = newTransaction( AUTH_DISABLED );
        int labelId1 = transaction.tokenWrite().labelGetOrCreateForName( "Person" );
        int propertyKeyId1 = transaction.tokenWrite().propertyKeyGetOrCreateForName( "foo" );
        LabelSchemaDescriptor personFooDescriptor = forLabel( labelId1, propertyKeyId1 );
        transaction.schemaWrite().indexCreate( personFooDescriptor, "person foo index" );
        commit();

        try ( org.neo4j.graphdb.Transaction tx = db.beginTx() )
        {
            // When & Then
            // this will always be true because that procedure returns void BUT it proves that it runs on system
            assertFalse( tx.execute( "CALL db.resampleIndex('Person')" ).hasNext());
        }
    }

    @Test
    void resampleOutdatedIndexes() throws Throwable
    {
        // Given
        KernelTransaction transaction = newTransaction( AUTH_DISABLED );
        int labelId1 = transaction.tokenWrite().labelGetOrCreateForName( "Person" );
        int propertyKeyId1 = transaction.tokenWrite().propertyKeyGetOrCreateForName( "foo" );
        LabelSchemaDescriptor personFooDescriptor = forLabel( labelId1, propertyKeyId1 );
        transaction.schemaWrite().indexCreate( personFooDescriptor, "person foo index" );
        commit();

        try ( org.neo4j.graphdb.Transaction tx = db.beginTx() )
        {
            // When & Then
            // this will always be true because that procedure returns void BUT it proves that it runs on system
            assertFalse( tx.execute( "CALL db.resampleOutdatedIndexes" ).hasNext());
        }
    }

    @Test
    void awaitEventuallyConsistentIndexRefresh() throws Throwable
    {
        // Given
        KernelTransaction transaction = newTransaction( AUTH_DISABLED );
        int labelId1 = transaction.tokenWrite().labelGetOrCreateForName( "Person" );
        int propertyKeyId1 = transaction.tokenWrite().propertyKeyGetOrCreateForName( "foo" );
        LabelSchemaDescriptor personFooDescriptor = forLabel( labelId1, propertyKeyId1 );
        transaction.schemaWrite().indexCreate( personFooDescriptor, "person foo index" );
        commit();

        try ( org.neo4j.graphdb.Transaction tx = db.beginTx() )
        {
            // When & Then
            // this will always be true because that procedure returns void BUT it proves that it runs on system
            assertFalse( tx.execute( "CALL db.index.fulltext.awaitEventuallyConsistentIndexRefresh" ).hasNext());
        }
    }

    @Test
    void fulltextAwaitIndex()
    {
        // Don't need any setup because creating those indexes is also faked on system so we cannot test against it

        try ( org.neo4j.graphdb.Transaction tx = db.beginTx() )
        {
            // When & Then
            // this will always be true because that procedure returns void BUT it proves that it runs on system
            assertFalse( tx.execute( "CALL db.index.fulltext.awaitIndex('A',10)" ).hasNext());
        }
    }

    @Test
    void queryNodes()
    {
        // Don't need any setup because creating those indexes is also faked on system so we cannot test against it

        try ( org.neo4j.graphdb.Transaction tx = db.beginTx() )
        {
            // When & Then
            assertFalse( tx.execute( "CALL db.index.fulltext.queryNodes('businessNameIndex', 'pizza')" ).hasNext());
        }
    }

    @Test
    void queryRelationships()
    {
        // Don't need any setup because creating those indexes is also faked on system so we cannot test against it

        try ( org.neo4j.graphdb.Transaction tx = db.beginTx() )
        {
            // When & Then
            assertFalse( tx.execute( "CALL db.index.fulltext.queryRelationships('businessNameIndex', 'pizza')" ).hasNext());
        }
    }

    @Test
    void nodeTypeProperties() throws Throwable
    {
        KernelTransaction transaction = newTransaction( AUTH_DISABLED );
        long nodeId = transaction.dataWrite().nodeCreate();
        transaction.dataWrite().nodeCreate();
        int propId = transaction.tokenWrite().propertyKeyGetOrCreateForName( "greeting" );
        transaction.dataWrite().nodeSetProperty( nodeId,propId, stringValue( "Hi!" ) );
        commit();

        try ( org.neo4j.graphdb.Transaction tx = db.beginTx() )
        {
            // When & Then
            assertFalse( tx.execute( "CALL db.schema.nodeTypeProperties" ).hasNext());
        }
    }

    @Test
    void relTypeProperties() throws Throwable
    {
        KernelTransaction transaction = newTransaction( AUTH_DISABLED );
        int type = transaction.tokenWrite().relationshipTypeGetOrCreateForName( "REL" );
        int propId = transaction.tokenWrite().propertyKeyGetOrCreateForName( "greeting" );
        long nodeId = transaction.dataWrite().nodeCreate();
        long relId = transaction.dataWrite().relationshipCreate( nodeId, type, nodeId );
        transaction.dataWrite().relationshipCreate( nodeId, type, nodeId );
        transaction.dataWrite().relationshipSetProperty( relId,propId, stringValue( "Hi!" ) );
        commit();

        try ( org.neo4j.graphdb.Transaction tx = db.beginTx() )
        {
            // When & Then
            assertFalse( tx.execute( "CALL db.schema.relTypeProperties" ).hasNext());
        }
    }

    @Test
    void schemaVisualization() throws Throwable
    {
        KernelTransaction transaction = newTransaction( AUTH_DISABLED );
        int type = transaction.tokenWrite().relationshipTypeGetOrCreateForName( "REL" );
        int propId = transaction.tokenWrite().propertyKeyGetOrCreateForName( "greeting" );
        long nodeId = transaction.dataWrite().nodeCreate();
        long relId = transaction.dataWrite().relationshipCreate( nodeId, type, nodeId );
        transaction.dataWrite().relationshipCreate( nodeId, type, nodeId );
        transaction.dataWrite().relationshipSetProperty( relId,propId, stringValue( "Hi!" ) );
        commit();

        try ( org.neo4j.graphdb.Transaction tx = db.beginTx() )
        {
            // When & Then
            assertFalse( tx.execute( "CALL db.schema.visualization" ).hasNext());
        }
    }

    @Test
    void stats()
    {

        try ( org.neo4j.graphdb.Transaction tx = db.beginTx() )
        {
            // When & Then
            assertFalse( tx.execute( "CALL db.stats.collect('QUERIES')" ).hasNext());
            assertFalse( tx.execute( "CALL db.stats.clear('QUERIES')" ).hasNext());
            assertFalse( tx.execute( "CALL db.stats.retrieve('TOKENS')" ).hasNext());
            assertFalse( tx.execute( "CALL db.stats.retrieveAllAnonymized('myGraphToken')" ).hasNext());
            assertFalse( tx.execute( "CALL db.stats.status" ).hasNext());
            assertFalse( tx.execute( "CALL db.stats.stop('QUERIES')" ).hasNext());
        }

        db.executeTransactionally( "CREATE USER bar SET PASSWORD 'f00' CHANGE NOT REQUIRED" );

        try ( org.neo4j.graphdb.Transaction tx = db.beginTx() )
        {
            // When & Then
            assertFalse( tx.execute( "CALL db.stats.collect('QUERIES')" ).hasNext());
            assertFalse( tx.execute( "CALL db.stats.clear('QUERIES')" ).hasNext());
            assertFalse( tx.execute( "CALL db.stats.retrieve('TOKENS')" ).hasNext());
            assertFalse( tx.execute( "CALL db.stats.retrieveAllAnonymized('myGraphToken')" ).hasNext());
            assertFalse( tx.execute( "CALL db.stats.status" ).hasNext());
            assertFalse( tx.execute( "CALL db.stats.stop('QUERIES')" ).hasNext());
        }
    }

    @Test
    void prepareForReplanningShouldHaveEmptyResult()
    {
        try ( org.neo4j.graphdb.Transaction tx = db.beginTx() )
        {
            // When & Then
            // this will always be true because that procedure returns void BUT it proves that it runs on system
            assertFalse( tx.execute( "CALL db.prepareForReplanning" ).hasNext());
        }
    }

    @Test
    void checkCommunityProceduresThatAreNotAllowedOnSystem()
    {
        List<String> queries = List.of(
                "CALL db.createIndex('MyIndex', ':Person(name)', 'lucene+native-2.0')",
                "CALL db.createLabel('Foo')",
                "CALL db.createProperty('bar')",
                "CALL db.createRelationshipType('BAZ')",
                "CALL db.createUniquePropertyConstraint('MyConstraint', ':Person(name)', 'lucene+native-2.0')",
                "CALL db.index.fulltext.createNodeIndex('businessNameIndex', ['Business'],['name'])",
                "CALL db.index.fulltext.createRelationshipIndex('businessNameIndex', ['Business'],['name'])",
                "CALL dbms.setTXMetaData( { User: 'Sascha' } )",
                "CALL db.index.fulltext.drop('businessNameIndex')");

        for ( String q : queries )
        {
            try ( org.neo4j.graphdb.Transaction tx = db.beginTx() )
            {
                // When & Then
                RuntimeException exception = assertThrows( RuntimeException.class, () -> tx.execute( q ) );
                assertTrue( exception.getMessage().startsWith( "Not a recognised system command or procedure:" ),
                        "Wrong error message for '" + q + "' => " + exception.getMessage() );
            }
        }
    }

    @Test
    void failWhenCallingNonExistingProcedures()
    {
        assertThrows( ProcedureException.class,
                () -> dbmsOperations().procedureCallDbms( -1, new AnyValue[0], transaction,
                        dependencyResolver, AnonymousContext.access().authorize(
                        LoginContext.IdLookup.EMPTY, getDatabaseName() ), EMPTY_RESOURCE_MANAGER, new DefaultValueMapper( transaction ) ) );
    }

    @Test
    void failWhenCallingNonSystemProcedures()
    {
        assertThrows( RuntimeException.class, () -> {
            try ( org.neo4j.graphdb.Transaction tx = db.beginTx() )
            {
                tx.execute( "CALL db.createLabel('foo')" );
            }
        } );
    }
}
