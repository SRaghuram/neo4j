/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.dbstructure;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeUnit;

import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.kernel.api.TokenRead;
import org.neo4j.internal.schema.ConstraintDescriptor;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.IndexPrototype;
import org.neo4j.internal.schema.IndexProviderDescriptor;
import org.neo4j.internal.schema.SchemaDescriptor;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.kernel.impl.util.dbstructure.DbStructureVisitor;
import org.neo4j.kernel.impl.util.dbstructure.GraphDbStructureGuide;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.extension.ImpermanentDbmsExtension;
import org.neo4j.test.extension.Inject;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.neo4j.graphdb.Label.label;
import static org.neo4j.graphdb.RelationshipType.withName;
import static org.neo4j.internal.kernel.api.TokenRead.ANY_LABEL;
import static org.neo4j.internal.kernel.api.TokenRead.ANY_RELATIONSHIP_TYPE;

@ImpermanentDbmsExtension
class GraphDbStructureGuideTest
{
    @Test
    void visitsLabelIds()
    {
        // GIVEN
        DbStructureVisitor visitor = mock( DbStructureVisitor.class );
        tx.createNode( label("Person") );
        tx.createNode( label("Party") );
        tx.createNode( label("Animal") );
        int personLabelId;
        int partyLabelId;
        int animalLabelId;

        KernelTransaction ktx = ((InternalTransaction) tx).kernelTransaction();
        TokenRead tokenRead = ktx.tokenRead();
        personLabelId = tokenRead.nodeLabel( "Person" );
        partyLabelId = tokenRead.nodeLabel( "Party" );
        animalLabelId = tokenRead.nodeLabel( "Animal" );

        // WHEN
        accept( visitor );

        // THEN
        verify( visitor ).visitLabel( personLabelId, "Person" );
        verify( visitor ).visitLabel( partyLabelId, "Party" );
        verify( visitor ).visitLabel( animalLabelId, "Animal" );
    }

    @Test
    void visitsPropertyKeyIds() throws Exception
    {
        // GIVEN
        DbStructureVisitor visitor = mock( DbStructureVisitor.class );
        int nameId = createPropertyKey( "name" );
        int ageId = createPropertyKey( "age" );
        int osId = createPropertyKey( "os" );

        // WHEN
        accept( visitor );

        // THEN
        verify( visitor ).visitPropertyKey( nameId, "name" );
        verify( visitor ).visitPropertyKey( ageId, "age" );
        verify( visitor ).visitPropertyKey( osId, "os" );
    }

    @Test
    void visitsRelationshipTypeIds()
    {
        // GIVEN
        DbStructureVisitor visitor = mock( DbStructureVisitor.class );
        Node lhs = tx.createNode();
        Node rhs = tx.createNode();
        lhs.createRelationshipTo( rhs, withName( "KNOWS" ) );
        lhs.createRelationshipTo( rhs, withName( "LOVES" ) );
        lhs.createRelationshipTo( rhs, withName( "FAWNS_AT" ) );
        int knowsId;
        int lovesId;
        int fawnsAtId;
        KernelTransaction ktx = ((InternalTransaction) tx).kernelTransaction();

        TokenRead tokenRead = ktx.tokenRead();
        knowsId = tokenRead.relationshipType( "KNOWS" );
        lovesId = tokenRead.relationshipType( "LOVES" );
        fawnsAtId = tokenRead.relationshipType( "FAWNS_AT" );

        // WHEN
        accept( visitor );

        // THEN
        verify( visitor ).visitRelationshipType( knowsId, "KNOWS" );
        verify( visitor ).visitRelationshipType( lovesId, "LOVES" );
        verify( visitor ).visitRelationshipType( fawnsAtId, "FAWNS_AT" );
    }

    @Test
    void visitsIndexes() throws Exception
    {
        DbStructureVisitor visitor = mock( DbStructureVisitor.class );
        int labelId = createLabel( "Person" );
        int pkId = createPropertyKey( "name" );

        commitAndReOpen();

        IndexDescriptor reference = createSchemaIndex( labelId, pkId );

        // WHEN
        accept( visitor );

        // THEN
        verify( visitor ).visitIndex( reference, "(:Person {name})", 1.0d, 0L );
    }

    @Test
    void visitsUniqueConstraintsAndIndices() throws Exception
    {
        DbStructureVisitor visitor = mock( DbStructureVisitor.class );
        int labelId = createLabel( "Person" );
        int pkId = createPropertyKey( "name" );

        commitAndReOpen();

        ConstraintDescriptor constraint = createUniqueConstraint( labelId, pkId );
        GraphDatabaseSettings.SchemaIndex providerSetting = GraphDatabaseSettings.SchemaIndex.NATIVE_BTREE10;
        IndexProviderDescriptor indexProvider = new IndexProviderDescriptor( providerSetting.providerKey(), providerSetting.providerVersion() );
        IndexDescriptor descriptor = IndexPrototype.uniqueForSchema( constraint.schema(), indexProvider ).withName( constraint.getName() ).materialise( 1 );

        // WHEN
        accept( visitor );

        // THEN
        verify( visitor ).visitIndex( descriptor, "(:Person {name})", 1.0d, 0L );
        verify( visitor ).visitUniqueConstraint( constraint.asUniquenessConstraint(),
                "Constraint( id=2, name='constraint_e26b1a8b', type='UNIQUENESS', schema=(:Person {name}), ownedIndex=1 )" );
    }

    @Test
    void visitsNodeCounts() throws Exception
    {
        // GIVEN
        DbStructureVisitor visitor = mock( DbStructureVisitor.class );
        int personLabelId = createLabeledNodes( "Person", 40 );
        int partyLabelId = createLabeledNodes( "Party", 20 );
        int animalLabelId = createLabeledNodes( "Animal", 30 );

        // WHEN
        accept( visitor );

        // THEN
        verify( visitor ).visitAllNodesCount( 90 );
        verify( visitor).visitNodeCount( personLabelId, "Person", 40 );
        verify( visitor ).visitNodeCount( partyLabelId, "Party", 20 );
        verify( visitor ).visitNodeCount( animalLabelId, "Animal", 30 );
    }

    @Test
    void visitsRelCounts() throws Exception
    {
        // GIVEN
        DbStructureVisitor visitor = mock( DbStructureVisitor.class );

        int personLabelId = createLabeledNodes( "Person", 40 );
        int partyLabelId = createLabeledNodes( "Party", 20 );

        int knowsId = createRelTypeId( "KNOWS" );
        int lovesId = createRelTypeId( "LOVES" );

        long personNode = createLabeledNode( personLabelId );
        long partyNode = createLabeledNode( partyLabelId );

        createRel( personNode, knowsId, personNode );
        createRel( personNode, lovesId, partyNode );

        // WHEN
        accept( visitor );

        // THEN
        verify( visitor ).visitRelCount( ANY_LABEL, knowsId, ANY_LABEL, "MATCH ()-[:KNOWS]->() RETURN count(*)", 1L );
        verify( visitor ).visitRelCount( ANY_LABEL, lovesId, ANY_LABEL, "MATCH ()-[:LOVES]->() RETURN count(*)", 1L );
        verify( visitor ).visitRelCount( ANY_LABEL, ANY_LABEL, ANY_LABEL, "MATCH ()-[]->() RETURN count(*)", 2L );

        verify( visitor ).visitRelCount( personLabelId, knowsId, ANY_LABEL, "MATCH (:Person)-[:KNOWS]->() RETURN count(*)", 1L );
        verify( visitor ).visitRelCount( ANY_LABEL, knowsId, personLabelId, "MATCH ()-[:KNOWS]->(:Person) RETURN count(*)", 1L );

        verify( visitor ).visitRelCount( personLabelId, lovesId, ANY_LABEL, "MATCH (:Person)-[:LOVES]->() RETURN count(*)", 1L );
        verify( visitor ).visitRelCount( ANY_LABEL, lovesId, personLabelId, "MATCH ()-[:LOVES]->(:Person) RETURN count(*)", 0L );

        verify( visitor ).visitRelCount( personLabelId, ANY_RELATIONSHIP_TYPE, ANY_LABEL, "MATCH (:Person)-[]->() RETURN count(*)", 2L );
        verify( visitor ).visitRelCount( ANY_LABEL, ANY_RELATIONSHIP_TYPE, personLabelId, "MATCH ()-[]->(:Person) RETURN count(*)", 1L );

        verify( visitor ).visitRelCount( partyLabelId, knowsId, ANY_LABEL, "MATCH (:Party)-[:KNOWS]->() RETURN count(*)", 0L );
        verify( visitor ).visitRelCount( ANY_LABEL, knowsId, partyLabelId, "MATCH ()-[:KNOWS]->(:Party) RETURN count(*)", 0L );

        verify( visitor ).visitRelCount( partyLabelId, lovesId, ANY_LABEL, "MATCH (:Party)-[:LOVES]->() RETURN count(*)", 0L );
        verify( visitor ).visitRelCount( ANY_LABEL, lovesId, partyLabelId, "MATCH ()-[:LOVES]->(:Party) RETURN count(*)", 1L );

        verify( visitor ).visitRelCount( partyLabelId, ANY_RELATIONSHIP_TYPE, ANY_LABEL, "MATCH (:Party)-[]->() RETURN count(*)", 0L );
        verify( visitor ).visitRelCount( ANY_LABEL, ANY_RELATIONSHIP_TYPE, partyLabelId, "MATCH ()-[]->(:Party) RETURN count(*)", 1L );
    }

    private void createRel( long startId, int relTypeId, long endId ) throws Exception
    {
        KernelTransaction ktx = ((InternalTransaction) tx).kernelTransaction();

        ktx.dataWrite().relationshipCreate( startId, relTypeId, endId );
}

    private IndexDescriptor createSchemaIndex( int labelId, int pkId ) throws Exception
    {
        KernelTransaction ktx = ((InternalTransaction) tx).kernelTransaction();

        return ktx.schemaWrite().indexCreate( SchemaDescriptor.forLabel( labelId, pkId ), null );

    }

    private ConstraintDescriptor createUniqueConstraint( int labelId, int pkId ) throws Exception
    {
        KernelTransaction ktx = ((InternalTransaction) tx).kernelTransaction();

        return ktx.schemaWrite()
                .uniquePropertyConstraintCreate( IndexPrototype.uniqueForSchema( SchemaDescriptor.forLabel( labelId, pkId ) ) );
    }

    private int createLabeledNodes( String labelName, int amount ) throws Exception
    {
        int labelId = createLabel( labelName );
        for ( int i = 0; i < amount; i++ )
        {
            createLabeledNode( labelId );
        }
        return labelId;
    }

    private long createLabeledNode( int labelId ) throws Exception
    {
        KernelTransaction ktx = ((InternalTransaction) tx).kernelTransaction();

        long nodeId = ktx.dataWrite().nodeCreate();
        ktx.dataWrite().nodeAddLabel( nodeId, labelId );
        return nodeId;
    }

    private int createLabel( String name ) throws Exception
    {
        KernelTransaction ktx = ((InternalTransaction) tx).kernelTransaction();
        return ktx.tokenWrite().labelGetOrCreateForName( name );
    }

    private int createPropertyKey( String name ) throws Exception
    {
        KernelTransaction ktx = ((InternalTransaction) tx).kernelTransaction();
        return ktx.tokenWrite().propertyKeyGetOrCreateForName( name );
    }

    private int createRelTypeId( String name ) throws Exception
    {
        KernelTransaction ktx = ((InternalTransaction) tx).kernelTransaction();

        return ktx.tokenWrite().relationshipTypeGetOrCreateForName( name );
    }

    @Inject
    private GraphDatabaseAPI db;
    private Transaction tx;

    @BeforeEach
    void setUp()
    {
        this.tx = db.beginTx();
    }

    void commitAndReOpen()
    {
        commit();

        tx = db.beginTx();
    }

    void accept( DbStructureVisitor visitor )
    {
        commitAndReOpen();

        tx.schema().awaitIndexesOnline( 30, TimeUnit.SECONDS );
        commit();

        GraphDbStructureGuide analyzer = new GraphDbStructureGuide( db );
        analyzer.accept( visitor );
    }

    private void commit()
    {
        tx.commit();
    }
}
