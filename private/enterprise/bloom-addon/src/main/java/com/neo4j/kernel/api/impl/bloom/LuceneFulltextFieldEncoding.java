/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.kernel.api.impl.bloom;

import org.apache.lucene.document.Field;
import org.apache.lucene.document.TextField;

import org.neo4j.values.storable.Value;

class LuceneFulltextFieldEncoding
{
    static Field encodeField( String name, Value value )
    {
        String stringValue = value.prettyPrint();

        TextField field = new TextField( name, stringValue, Field.Store.NO );
        return field;
    }
}
