/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.tools.dump;

import com.neo4j.tools.dump.InconsistentRecords.Type;
import org.junit.jupiter.api.Test;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.StringReader;

import org.neo4j.consistency.RecordType;
import org.neo4j.consistency.report.InconsistencyMessageLogger;
import org.neo4j.consistency.store.synthetic.IndexEntry;
import org.neo4j.consistency.store.synthetic.TokenScanDocument;
import org.neo4j.internal.index.label.EntityTokenRange;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.IndexPrototype;
import org.neo4j.internal.schema.SchemaDescriptor;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.PropertyRecord;
import org.neo4j.kernel.impl.store.record.RelationshipGroupRecord;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.logging.Log;
import org.neo4j.logging.log4j.Log4jLogProvider;
import org.neo4j.test.InMemoryTokens;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.common.EntityType.NODE;
import static org.neo4j.common.EntityType.RELATIONSHIP;
import static org.neo4j.internal.schema.SchemaDescriptor.forLabel;

class InconsistencyReportReaderTest
{
    @Test
    void shouldReadBasicEntities() throws Exception
    {
        // GIVEN
        ByteArrayOutputStream out = new ByteArrayOutputStream( 1_000 );
        Log log = new Log4jLogProvider( out ).getLog( "test" );
        InconsistencyMessageLogger logger = new InconsistencyMessageLogger( log );
        InMemoryTokens tokens = new InMemoryTokens().label( 1, "label" ).propertyKey( 1, "prop" );
        long nodeId = 1;
        long indexNodeId = 2;
        long nodeNotInTheIndexId = 3;
        long nodeNotInTheLabelIndexId = 4;
        long indexId = 5;
        long relationshipGroupId = 6;
        long relationshipId = 7;
        long relationshipNotInTypeIndexId = 8;
        long propertyId = 9;
        logger.error( RecordType.NODE, new NodeRecord( nodeId ),
                "Some error", "something" );
        logger.error( RecordType.RELATIONSHIP, new RelationshipRecord( relationshipId ),
                "Some error", "something" );
        logger.error( RecordType.RELATIONSHIP_GROUP, new RelationshipGroupRecord( relationshipGroupId ),
                "Some error", "something" );
        logger.error( RecordType.PROPERTY, new PropertyRecord( propertyId ),
                "Some error", "something" );
        logger.error( RecordType.INDEX, new IndexEntry( someIndexDescriptor(), tokens, indexNodeId ), "Some index error",
                "Something wrong with index" );
        logger.error( RecordType.NODE, new NodeRecord( nodeNotInTheIndexId ), "Some index error",
                      IndexPrototype.forSchema( forLabel( 1, 2 ) ).withName( "index_" + indexId ).materialise( indexId ).userDescription( tokens ) );
        logger.error( RecordType.LABEL_SCAN_DOCUMENT, new TokenScanDocument( new EntityTokenRange( 0, new long[0][], NODE ) ),
                "Some label index error", new NodeRecord( nodeNotInTheLabelIndexId ) );
        logger.error( RecordType.RELATIONSHIP_TYPE_SCAN_DOCUMENT, new TokenScanDocument( new EntityTokenRange( 0, new long[0][], RELATIONSHIP ) ),
                "Some relationship type error", new RelationshipRecord( relationshipNotInTypeIndexId ) );
        String text = out.toString();

        // WHEN
        InconsistentRecords inconsistencies = new InconsistentRecords();
        InconsistencyReportReader reader = new InconsistencyReportReader( inconsistencies );
        reader.read( new BufferedReader( new StringReader( text ) ) );

        // THEN
        assertTrue( inconsistencies.containsId( Type.NODE, nodeId ) );
        assertTrue( inconsistencies.containsId( Type.NODE, indexNodeId ) );
        assertTrue( inconsistencies.containsId( Type.NODE, nodeNotInTheIndexId ) );
        assertTrue( inconsistencies.containsId( Type.NODE, nodeNotInTheLabelIndexId ) );
        assertTrue( inconsistencies.containsId( Type.RELATIONSHIP, relationshipId ) );
        assertTrue( inconsistencies.containsId( Type.RELATIONSHIP, relationshipNotInTypeIndexId ) );
        assertTrue( inconsistencies.containsId( Type.RELATIONSHIP_GROUP, relationshipGroupId ) );
        assertTrue( inconsistencies.containsId( Type.PROPERTY, propertyId ) );
        assertTrue( inconsistencies.containsId( Type.SCHEMA_INDEX, indexId ) );
    }

    @Test
    void shouldParseRelationshipGroupInconsistencies() throws Exception
    {
        // Given
        InconsistentRecords inconsistencies = new InconsistentRecords();
        String text =
                "ERROR: The first outgoing relationship is not the first in its chain.\n" +
                "\tRelationshipGroup[1337,type=1,out=2,in=-1,loop=-1,prev=-1,next=3,used=true,owner=4,secondaryUnitId=-1]\n" +
                "ERROR: The first outgoing relationship is not the first in its chain.\n" +
                "\tRelationshipGroup[4242,type=1,out=2,in=-1,loop=-1,prev=-1,next=3,used=true,owner=4,secondaryUnitId=-1]\n";

        // When
        InconsistencyReportReader reader = new InconsistencyReportReader( inconsistencies );
        reader.read( new BufferedReader( new StringReader( text ) ) );

        // Then
        assertTrue( inconsistencies.containsId( Type.RELATIONSHIP_GROUP, 1337 ) );
        assertTrue( inconsistencies.containsId( Type.RELATIONSHIP_GROUP, 4242 ) );
    }

    private IndexDescriptor someIndexDescriptor()
    {
        return IndexPrototype.forSchema( SchemaDescriptor.forLabel( 1, 1 ) ).withName( "index_1" ).materialise( 1L );
    }
}
