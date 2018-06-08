/*
 * Copyright (c) 2002-2018 "Neo4j,"
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
package org.neo4j.kernel.builtinprocs;

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

import org.neo4j.internal.kernel.api.CursorFactory;
import org.neo4j.internal.kernel.api.LabelSet;
import org.neo4j.internal.kernel.api.NamedToken;
import org.neo4j.internal.kernel.api.NodeCursor;
import org.neo4j.internal.kernel.api.PropertyCursor;
import org.neo4j.internal.kernel.api.Read;
import org.neo4j.internal.kernel.api.SchemaRead;
import org.neo4j.internal.kernel.api.TokenRead;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.ValueGroup;

public class SchemaCalculator
{
    private GraphDatabaseAPI db;
    private KernelTransaction ktx;

    private Map<LabelSet,Set<Integer>> labelSetToPropertyKeysMapping;
    // TODO: make those different Set<whatever> etc. into more useful/understandable classes?!
    private Table<LabelSet,Integer,Object> labelSetAndNodePropertyKeyIdToValueTypeMapping;  // Dislike object here...
    private Map<Integer,String> labelIdToLabelNameMapping;
    private Map<Integer,String> propertyIdToPropertylNameMapping;

    private final Set<Integer> emptyPropertyIdSet = Collections.unmodifiableSet( Collections.emptySet() );

    private final String ANYVALUE = "ANYVALUE";
    private final String NODE = "Node";
    private final String RELATIONSHIP = "Relationship";

    //TODO: Change Table to Pair!
    //TODO: Remove Guava from pom

    public SchemaCalculator( GraphDatabaseAPI db, KernelTransaction ktx )
    {
        this.db = db;
        this.ktx = ktx;
        labelSetAndNodePropertyKeyIdToValueTypeMapping = HashBasedTable.create();
    }

    public Stream<BuiltInSchemaProcedures.SchemaInfoResult> calculateTabularResultStream()
    {
        calculateSchema();

        // Build up a stream of SchemaInfoResult objects from calculated mappings
        List<BuiltInSchemaProcedures.SchemaInfoResult> results = new ArrayList<>();
        for ( LabelSet labelSet : labelSetToPropertyKeysMapping.keySet() )
        {
            // lookup label names and produce list of names
            List<String> labelNames = new ArrayList<>();
            for ( int i = 0; i < labelSet.numberOfLabels(); i++ )
            {
                String name = labelIdToLabelNameMapping.get( labelSet.label( i ) );
                labelNames.add( name );
            }

            Set<Integer> propertyIds = labelSetToPropertyKeysMapping.get( labelSet );

            if ( propertyIds.size() == 0 )
            {
                results.add( new BuiltInSchemaProcedures.SchemaInfoResult( NODE, labelNames, null, null ) );
            }
            else
            {
                for ( Integer propId : propertyIds )
                {
                    // lookup propId name and valueGroup
                    String propName = propertyIdToPropertylNameMapping.get( propId );
                    Object valueType = labelSetAndNodePropertyKeyIdToValueTypeMapping.get( labelSet, propId );

                    String cypherType;
                    if ( valueType instanceof Value )
                    {
                        cypherType = ((Value) valueType).getTypeName().toUpperCase();
                    }
                    else if ( valueType instanceof ValueGroup )
                    {
                        cypherType = ((ValueGroup) valueType).name();
                    }
                    else
                    {
                        cypherType = valueType.toString();
                    }

                    results.add( new BuiltInSchemaProcedures.SchemaInfoResult( NODE, labelNames, propName, cypherType ) );
                }
            }
        }

        return results.stream();
    }

    public Stream<SchemaProcedure.GraphResult> calculateGraphResultStream()
    {
        calculateSchema();
        //TODO finish this
        throw new NotImplementedException();
    }

    private void calculateSchema() //TODO: Parallelize this: (Nodes | Rels) and/or more
    {
        // this one does most of the work
        //TODO: implement this for rels
        try ( Statement ignore = ktx.acquireStatement() )
        {
            Read dataRead = ktx.dataRead();
            TokenRead tokenRead = ktx.tokenRead();
            SchemaRead schemaRead = ktx.schemaRead();
            CursorFactory cursors = ktx.cursors();

            // setup mappings
            int labelCount = tokenRead.labelCount();
            labelSetToPropertyKeysMapping = new HashMap<>( labelCount );
            labelIdToLabelNameMapping = new HashMap<>( labelCount );
            propertyIdToPropertylNameMapping = new HashMap<>( tokenRead.propertyKeyCount() );

            NodeCursor nodeCursor = cursors.allocateNodeCursor();
            dataRead.allNodesScan( nodeCursor );

            while ( nodeCursor.next() )
            {
                // each node
                LabelSet labels = nodeCursor.labels();
                PropertyCursor propertyCursor = cursors.allocatePropertyCursor();
                nodeCursor.properties( propertyCursor );
                Set<Integer> propertyIds = new HashSet<>();  // is Set really the best fit here?

                while ( propertyCursor.next() )
                {
                    // each property
                    Value currentValue = propertyCursor.propertyValue(); //1st check
                    int propertyKeyId = propertyCursor.propertyKey();

                    Object typeExampleValue = labelSetAndNodePropertyKeyIdToValueTypeMapping.get( labels, propertyKeyId );
                    if ( typeExampleValue == null )
                    {
                        typeExampleValue = currentValue;
                    }
                    else
                    {
                        // Object value in labelSetAndNodePropertyKeyIdToValueTypeMapping can be either:
                        // A) Value, if the Values match on class level
                        // B) ValueGroup, if the ValueGroups of the Values match
                        // C) String, if nothing matches

                        // Are they the same value typ? -[no]-> Are they the same ValueGroup? -[no]-> AnyValue

                        // TODO: We could check for String first and could skip the other instanceof checks (has Pro/Cons)

                        if ( typeExampleValue instanceof Value )
                        {
                            // check if classes match
                            if ( !currentValue.getClass().equals( typeExampleValue.getClass() ) )
                            {
                                // Clases don't match -> update needed
                                if ( currentValue.valueGroup().equals( ((Value) typeExampleValue).valueGroup() ) )
                                {
                                    // same valueGroup -> set that
                                    typeExampleValue = currentValue.valueGroup();
                                }
                                else
                                {
                                    // Not same valuegroup -> set to AnyValue
                                    typeExampleValue = ANYVALUE;
                                }
                            }
                        }
                        else if ( typeExampleValue instanceof ValueGroup )
                        {
                            if ( !currentValue.valueGroup().equals( typeExampleValue ) )
                            {
                                // not same valueGroup -> update to AnyValue
                                typeExampleValue = ANYVALUE;
                            }
                        }
                    }

                    labelSetAndNodePropertyKeyIdToValueTypeMapping.put( labels, propertyKeyId, typeExampleValue );
                    propertyIds.add( propertyKeyId );
                }

                Set<Integer> oldPropertyKeySet = labelSetToPropertyKeysMapping.getOrDefault( labels, emptyPropertyIdSet );
                propertyIds.addAll( oldPropertyKeySet );
                labelSetToPropertyKeysMapping.put( labels, propertyIds );
            }

            // go through all labels
            Iterator<NamedToken> labelIterator = tokenRead.labelsGetAllTokens();
            while ( labelIterator.hasNext() )
            {
                NamedToken label = labelIterator.next();
                labelIdToLabelNameMapping.put( label.id(), label.name() );
            }

            // go through all propertyKeys
            Iterator<NamedToken> propIterator = tokenRead.propertyKeyGetAllTokens();
            while ( propIterator.hasNext() )
            {
                NamedToken prop = propIterator.next();
                propertyIdToPropertylNameMapping.put( prop.id(), prop.name() );
            }
        }
    }
}
