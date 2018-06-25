/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.kernel.api.impl.bloom;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.LongField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.Term;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import static org.apache.lucene.document.Field.Store.NO;
import static org.apache.lucene.document.Field.Store.YES;
import static com.neo4j.kernel.api.impl.bloom.FulltextProvider.LUCENE_FULLTEXT_ADDON_PREFIX;

class FulltextIndexConfiguration
{
    private static final String FIELD_METADATA_DOC = LUCENE_FULLTEXT_ADDON_PREFIX + "metadata__doc__field__";
    private static final String FIELD_CONFIG_ANALYZER = LUCENE_FULLTEXT_ADDON_PREFIX + "analyzer";
    private static final String FIELD_CONFIG_PROPERTIES = LUCENE_FULLTEXT_ADDON_PREFIX + "properties";
    static Term TERM = new Term( FIELD_METADATA_DOC );

    private final Set<String> properties;
    private final String analyzerClassName;

    FulltextIndexConfiguration( Document doc )
    {
        properties = new HashSet<>( Arrays.asList( doc.getValues( FIELD_CONFIG_PROPERTIES ) ) );
        analyzerClassName = doc.get( FIELD_CONFIG_ANALYZER );
    }

    FulltextIndexConfiguration( String analyzerClassName, Set<String> properties )
    {
        this.properties = properties;
        this.analyzerClassName = analyzerClassName;
    }

    @Override
    public boolean equals( Object o )
    {
        if ( this == o )
        {
            return true;
        }
        if ( o == null || getClass() != o.getClass() )
        {
            return false;
        }

        FulltextIndexConfiguration that = (FulltextIndexConfiguration) o;

        return properties.equals( that.properties ) &&
               analyzerClassName.equals( that.analyzerClassName );
    }

    @Override
    public int hashCode()
    {
        int result = properties.hashCode();
        result = 31 * result + analyzerClassName.hashCode();
        return result;
    }

    Document asDocument()
    {
        Document doc = new Document();
        doc.add( new StringField( FIELD_METADATA_DOC, "", NO ) );
        doc.add( new StoredField( FIELD_CONFIG_ANALYZER, analyzerClassName ) );
        for ( String property : properties )
        {
            doc.add( new StoredField( FIELD_CONFIG_PROPERTIES, property ) );
        }
        return doc;
    }

    public Set<String> getProperties()
    {
        return properties;
    }
}
