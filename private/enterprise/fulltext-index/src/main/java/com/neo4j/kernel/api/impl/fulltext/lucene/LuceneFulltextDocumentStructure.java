/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.kernel.api.impl.fulltext.lucene;

import com.neo4j.kernel.api.impl.fulltext.FulltextAdapter;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.Term;

import java.util.Collection;
import java.util.Iterator;

import org.neo4j.values.storable.Value;

import static org.apache.lucene.document.Field.Store.NO;

public class LuceneFulltextDocumentStructure
{
    private static final ThreadLocal<DocWithId> perThreadDocument = ThreadLocal.withInitial( DocWithId::new );

    private LuceneFulltextDocumentStructure()
    {
    }

    private static DocWithId reuseDocument( long id )
    {
        DocWithId doc = perThreadDocument.get();
        doc.setId( id );
        return doc;
    }

    public static Document documentRepresentingProperties( long id, Collection<String> propertyNames, Value[] values )
    {
        DocWithId document = reuseDocument( id );
        document.setValues( propertyNames, values );
        return document.document;
    }

    private static Field encodeValueField( String propertyKey, Value value )
    {
        String stringValue = value.prettyPrint();

        TextField field = new TextField( propertyKey, stringValue, NO );
        return field;
    }

    public static long getNodeId( Document from )
    {
        return Long.parseLong( from.get( FulltextAdapter.FIELD_ENTITY_ID ) );
    }

    public static Term newTermForChangeOrRemove( long id )
    {
        return new Term( FulltextAdapter.FIELD_ENTITY_ID, "" + id );
    }

    private static class DocWithId
    {
        private final Document document;

        private final Field idField;
        private final Field idValueField;

        private DocWithId()
        {
            idField = new StringField( FulltextAdapter.FIELD_ENTITY_ID, "", NO );
            idValueField = new NumericDocValuesField( FulltextAdapter.FIELD_ENTITY_ID, 0L );
            document = new Document();
            document.add( idField );
            document.add( idValueField );
        }

        private void setId( long id )
        {
            removeAllValueFields();
            idField.setStringValue( Long.toString( id ) );
            idValueField.setLongValue( id );
        }

        private void setValues( Collection<String> names, Value[] values )
        {
            int i = 0;
            for ( String name : names )
            {
                Value value = values[i++];
                if ( value != null )
                {
                    Field field = encodeValueField( name, value );
                    document.add( field );
                }
            }
        }

        private void removeAllValueFields()
        {
            Iterator<IndexableField> it = document.getFields().iterator();
            while ( it.hasNext() )
            {
                IndexableField field = it.next();
                String fieldName = field.name();
                if ( !fieldName.equals( FulltextAdapter.FIELD_ENTITY_ID ) )
                {
                    it.remove();
                }
            }
        }
    }
}
