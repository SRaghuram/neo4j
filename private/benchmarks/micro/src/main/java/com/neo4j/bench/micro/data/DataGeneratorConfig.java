/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.micro.data;

import com.neo4j.bench.model.model.Neo4jConfig;
import com.neo4j.bench.model.util.JsonUtil;
import com.neo4j.bench.micro.data.DataGenerator.GraphWriter;
import com.neo4j.bench.micro.data.DataGenerator.LabelLocality;
import com.neo4j.bench.micro.data.DataGenerator.Order;
import com.neo4j.bench.micro.data.DataGenerator.PropertyLocality;
import com.neo4j.bench.micro.data.DataGenerator.RelationshipLocality;

import java.nio.file.Path;
import java.text.NumberFormat;
import java.util.Arrays;
import java.util.Locale;
import java.util.Objects;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.RelationshipType;

public class DataGeneratorConfig
{
    private static final NumberFormat FORMAT = NumberFormat.getNumberInstance( Locale.ROOT );

    private int nodeCount;
    private String[] relationshipTypeNames;
    private int[] relationshipTypeCounts;
    private Order relationshipOrder;
    private RelationshipLocality relationshipLocality;
    private GraphWriter graphWriter;
    private PropertyDefinition[] nodeProperties;
    private PropertyDefinition[] relationshipProperties;
    private PropertyLocality propertyLocality;
    private Order propertyOrder;
    private String[] labelNames;
    private Order labelOrder;
    private LabelLocality labelLocality;
    private LabelKeyDefinition[] schemaIndexes;
    private LabelKeyDefinition[] uniqueConstraints;
    private LabelKeyDefinition[] mandatoryNodeConstraints;
    private RelationshipKeyDefinition[] mandatoryRelationshipConstraints;
    private LabelKeyDefinition[] fulltextNodeSchemaIndexes;
    private RelationshipKeyDefinition[] fulltextRelationshipSchemaIndexes;
    private Neo4jConfig neo4jConfig;
    private boolean isReusable;
    private String augmentedBy;
    private long rngSeed;

    @SuppressWarnings( "unused" )
    private DataGeneratorConfig()
    {
    }

    public DataGeneratorConfig(
            int nodeCount,
            RelationshipDefinition[] outRelationships,
            Order relationshipOrder,
            RelationshipLocality relationshipLocality,
            GraphWriter graphWriter,
            PropertyDefinition[] nodeProperties,
            PropertyDefinition[] relationshipProperties,
            PropertyLocality propertyLocality,
            Order propertyOrder,
            Label[] labels,
            Order labelOrder,
            LabelLocality labelLocality,
            LabelKeyDefinition[] schemaIndexes,
            LabelKeyDefinition[] uniqueConstraints,
            LabelKeyDefinition[] mandatoryNodeConstraints,
            RelationshipKeyDefinition[] mandatoryRelationshipConstraints,
            LabelKeyDefinition[] fulltextNodeSchemaIndexes,
            RelationshipKeyDefinition[] fulltextRelationshipSchemaIndexes,
            Neo4jConfig neo4jConfig,
            boolean isReusable,
            String augmentedBy,
            long rngSeed )
    {
        this.nodeCount = nodeCount;
        this.relationshipTypeNames = Stream.of( outRelationships )
                                           .map( RelationshipDefinition::type )
                                           .map( RelationshipType::name )
                                           .toArray( String[]::new );
        this.relationshipTypeCounts = Stream.of( outRelationships )
                                            .mapToInt( RelationshipDefinition::count )
                                            .toArray();
        this.relationshipOrder = relationshipOrder;
        this.relationshipLocality = relationshipLocality;
        this.graphWriter = graphWriter;
        this.nodeProperties = nodeProperties;
        this.relationshipProperties = relationshipProperties;
        this.propertyLocality = propertyLocality;
        this.propertyOrder = propertyOrder;
        this.labelNames = Stream.of( labels ).map( Label::name ).toArray( String[]::new );
        this.labelOrder = labelOrder;
        this.labelLocality = labelLocality;
        this.schemaIndexes = schemaIndexes;
        this.uniqueConstraints = uniqueConstraints;
        this.mandatoryNodeConstraints = mandatoryNodeConstraints;
        this.mandatoryRelationshipConstraints = mandatoryRelationshipConstraints;
        this.fulltextNodeSchemaIndexes = fulltextNodeSchemaIndexes;
        this.fulltextRelationshipSchemaIndexes = fulltextRelationshipSchemaIndexes;
        this.neo4jConfig = neo4jConfig;
        this.isReusable = isReusable;
        this.augmentedBy = augmentedBy;
        this.rngSeed = rngSeed;
    }

    public int nodeCount()
    {
        return nodeCount;
    }

    public int relationshipCount()
    {
        return outDegree() * nodeCount;
    }

    public int labelCount()
    {
        return nodeCount * labelNames.length;
    }

    public int nodePropertyCount()
    {
        return nodeProperties.length * nodeCount;
    }

    public int relationshipPropertyCount()
    {
        return relationshipProperties.length * relationshipCount();
    }

    public int outDegree()
    {
        return IntStream.of( relationshipTypeCounts ).sum();
    }

    public RelationshipDefinition[] outRelationships()
    {
        return IntStream.range( 0, relationshipTypeNames.length ).boxed()
                        .map( i -> new RelationshipDefinition(
                                RelationshipType.withName( relationshipTypeNames[i] ),
                                relationshipTypeCounts[i] ) )
                        .toArray( RelationshipDefinition[]::new );
    }

    public Order relationshipOrder()
    {
        return relationshipOrder;
    }

    public RelationshipLocality relationshipLocality()
    {
        return relationshipLocality;
    }

    public GraphWriter graphWriter()
    {
        return graphWriter;
    }

    public PropertyDefinition[] nodeProperties()
    {
        return nodeProperties;
    }

    public PropertyDefinition[] relationshipProperties()
    {
        return relationshipProperties;
    }

    public PropertyLocality propertyLocality()
    {
        return propertyLocality;
    }

    public Order propertyOrder()
    {
        return propertyOrder;
    }

    public Label[] labels()
    {
        return Stream.of( labelNames ).map( Label::label ).toArray( Label[]::new );
    }

    public Order labelOrder()
    {
        return labelOrder;
    }

    public LabelLocality labelLocality()
    {
        return labelLocality;
    }

    public LabelKeyDefinition[] schemaIndexes()
    {
        return schemaIndexes;
    }

    public LabelKeyDefinition[] uniqueConstraints()
    {
        return uniqueConstraints;
    }

    public LabelKeyDefinition[] mandatoryNodeConstraints()
    {
        return mandatoryNodeConstraints;
    }

    public RelationshipKeyDefinition[] mandatoryRelationshipConstraints()
    {
        return mandatoryRelationshipConstraints;
    }

    public LabelKeyDefinition[] fulltextNodeSchemaIndexes()
    {
        return fulltextNodeSchemaIndexes;
    }

    public RelationshipKeyDefinition[] fulltextRelationshipSchemaIndexes()
    {
        return fulltextRelationshipSchemaIndexes;
    }

    public Neo4jConfig neo4jConfig()
    {
        return neo4jConfig;
    }

    public boolean isReusable()
    {
        return isReusable;
    }

    public String augmentedBy()
    {
        return augmentedBy;
    }

    public long rngSeed()
    {
        return rngSeed;
    }

    public void serialize( Path file )
    {
        JsonUtil.serializeJson( file, this );
    }

    public static DataGeneratorConfig from( Path file )
    {
        return JsonUtil.deserializeJson( file, DataGeneratorConfig.class );
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
        DataGeneratorConfig that = (DataGeneratorConfig) o;
        return
                // re-usability is purposefully ignored
                // isReusable == that.isReusable &&

                // when augmentation key is equal, assume augmentation method is equivalent & deterministic
                augmentedBy.equals( that.augmentedBy ) &&

                nodeCount == that.nodeCount &&
                rngSeed == that.rngSeed &&
                Arrays.equals( relationshipTypeNames, that.relationshipTypeNames ) &&
                Arrays.equals( relationshipTypeCounts, that.relationshipTypeCounts ) &&
                relationshipOrder == that.relationshipOrder &&
                relationshipLocality == that.relationshipLocality &&
                graphWriter == that.graphWriter &&
                Arrays.equals( nodeProperties, that.nodeProperties ) &&
                Arrays.equals( relationshipProperties, that.relationshipProperties ) &&
                propertyLocality == that.propertyLocality &&
                propertyOrder == that.propertyOrder &&
                Arrays.equals( labelNames, that.labelNames ) &&
                labelOrder == that.labelOrder &&
                labelLocality == that.labelLocality &&
                Arrays.equals( schemaIndexes, that.schemaIndexes ) &&
                Arrays.equals( uniqueConstraints, that.uniqueConstraints ) &&
                Arrays.equals( mandatoryNodeConstraints, that.mandatoryNodeConstraints ) &&
                Arrays.equals( mandatoryRelationshipConstraints, that.mandatoryRelationshipConstraints ) &&
                Arrays.equals( fulltextNodeSchemaIndexes, that.fulltextNodeSchemaIndexes ) &&
                Arrays.equals( fulltextRelationshipSchemaIndexes, that.fulltextRelationshipSchemaIndexes ) &&
                Objects.equals( neo4jConfig, that.neo4jConfig );
    }

    @Override
    public int hashCode()
    {
        return Objects
                .hash( nodeCount, Arrays.hashCode( relationshipTypeNames ), Arrays.hashCode( relationshipTypeCounts ), relationshipOrder,
                       relationshipLocality,
                       graphWriter, Arrays.hashCode( nodeProperties ), Arrays.hashCode( relationshipProperties ), propertyLocality, propertyOrder,
                       Arrays.hashCode( labelNames ),
                       labelOrder, labelLocality, Arrays.hashCode( schemaIndexes ), Arrays.hashCode( uniqueConstraints ),
                       Arrays.hashCode( mandatoryNodeConstraints ),
                       Arrays.hashCode( mandatoryRelationshipConstraints ),
                       Arrays.hashCode( fulltextNodeSchemaIndexes ), Arrays.hashCode( fulltextRelationshipSchemaIndexes ),
                       isReusable, augmentedBy, rngSeed );
    }

    @Override
    public String toString()
    {
        return getClass().getSimpleName() + "\n" +
               "  Seed:                                " + rngSeed + "\n" +
               "  Is Reusable:                         " + isReusable + "\n" +
               "  Augmented By:                        " + augmentedBy + "\n" +
               "  Graph Writer:                        " + graphWriter + "\n" +
               "  ------ GLOBAL ------\n" +
               "  Node Count:                          " + FORMAT.format( nodeCount() ) + "\n" +
               "  Relationship Count:                  " + FORMAT.format( relationshipCount() ) + "\n" +
               "  Node Property Count:                 " + FORMAT.format( nodePropertyCount() ) + "\n" +
               "  Relationship Property Count:         " + FORMAT.format( relationshipPropertyCount() ) + "\n" +
               "  Label Count:                         " + FORMAT.format( labelCount() ) + "\n" +
               "  Index Count:                         " + FORMAT.format( schemaIndexes.length ) + "\n" +
               "  Indexes:                             " + Arrays.toString( schemaIndexes ) + "\n" +
               "  Uniqueness Constraint Count:         " + FORMAT.format( uniqueConstraints.length ) + "\n" +
               "  Uniqueness Constraints:              " + Arrays.toString( uniqueConstraints ) + "\n" +
               "  Mandatory Node Constraints:          " + Arrays.toString( mandatoryNodeConstraints ) + "\n" +
               "  Mandatory Relationship Constraints:  " + Arrays.toString( mandatoryRelationshipConstraints ) + "\n" +
               "  Fulltext Node Indexes:               " + Arrays.toString( fulltextNodeSchemaIndexes ) + "\n" +
               "  Fulltext Relationship Indexes:       " + Arrays.toString( fulltextRelationshipSchemaIndexes ) + "\n" +
               "  --- PER NODE/REL ---\n" +
               "  Out Relationship Count:              " + FORMAT.format( outDegree() ) + "\n" +
               "  Node Property Count:                 " + FORMAT.format( nodeProperties().length ) + "\n" +
               "  Node Properties :                    " + Arrays.toString( nodePropertyTypeStrings() ) + "\n" +
               "  Label Count:                         " + FORMAT.format( labels().length ) + "\n" +
               "  Labels:                              " + Arrays.toString( labels() ) + "\n" +
               "  Relationship Property Count:         " + FORMAT.format( relationshipProperties().length ) + "\n" +
               "  Relationship Properties :            " + Arrays.toString( relationshipPropertyTypeStrings() ) + "\n" +
               "  Relationship Types :                 " + Arrays.toString( relationshipTypeStrings() ) + "\n" +
               "  ----- LOCALITY -----\n" +
               "  Relationship Locality:               " + relationshipLocality + "\n" +
               "  Property Locality:                   " + propertyLocality + "\n" +
               "  Label Locality:                      " + labelLocality + "\n" +
               "  ----- ORDERING -----\n" +
               "  Property Order:                      " + propertyOrder + "\n" +
               "  Relationship Order                   " + relationshipOrder + "\n" +
               "  Label Order:                         " + labelOrder + "\n" +
               "  --------------------\n" +
               "  Neo4j Configuration:               \n" + neo4jConfig.toString() + "\n" +
               "  --------------------\n";
    }

    private String[] nodePropertyTypeStrings()
    {
        return propertyTypeStrings( nodeProperties );
    }

    private String[] relationshipPropertyTypeStrings()
    {
        return propertyTypeStrings( relationshipProperties );
    }

    private String[] propertyTypeStrings( PropertyDefinition[] properties )
    {
        return Stream.of( properties )
                     .map( PropertyDefinition::toString )
                     .toArray( String[]::new );
    }

    private String[] relationshipTypeStrings()
    {
        return Stream.of( outRelationships() )
                     .map( RelationshipDefinition::toString )
                     .toArray( String[]::new );
    }
}
