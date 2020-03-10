/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.micro.data;

import com.neo4j.bench.common.model.Neo4jConfig;
import com.neo4j.bench.micro.data.Augmenterizer.NullAugmenterizer;
import com.neo4j.bench.micro.data.DataGenerator.GraphWriter;
import com.neo4j.bench.micro.data.DataGenerator.LabelLocality;
import com.neo4j.bench.micro.data.DataGenerator.Order;
import com.neo4j.bench.micro.data.DataGenerator.PropertyLocality;
import com.neo4j.bench.micro.data.DataGenerator.RelationshipLocality;

import java.util.Map;
import java.util.stream.Stream;

import org.neo4j.graphdb.Label;

import static java.lang.String.format;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.counting;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.joining;

public class DataGeneratorConfigBuilder
{
    private int nodeCount;
    private RelationshipDefinition[] outRelationships = {};
    private Order relationshipOrder = Order.SHUFFLED;
    private RelationshipLocality relationshipLocality = RelationshipLocality.SCATTERED_BY_START_NODE;
    private GraphWriter graphWriter = GraphWriter.TRANSACTIONAL;
    private PropertyDefinition[] nodeProperties = {};
    private PropertyDefinition[] relationshipProperties = {};
    private PropertyLocality propertyLocality = PropertyLocality.SCATTERED_BY_ELEMENT;
    private Order propertyOrder = Order.SHUFFLED;
    private Label[] labels = {};
    private Order labelOrder = Order.SHUFFLED;
    private LabelLocality labelLocality = LabelLocality.SCATTERED_BY_NODE;
    private LabelKeyDefinition[] schemaIndexes = {};
    private LabelKeyDefinition[] uniqueConstraints = {};
    private LabelKeyDefinition[] mandatoryNodeConstraints = {};
    private RelationshipKeyDefinition[] mandatoryRelationshipConstraints = {};
    private LabelKeyDefinition[] fulltextNodeSchemaIndexes = {};
    private RelationshipKeyDefinition[] fulltextRelationshipSchemaIndexes = {};
    private Neo4jConfig neo4jConfig = Neo4jConfig.empty();
    private boolean isReusable;
    private String augmentedBy = NullAugmenterizer.AUGMENT_KEY;
    private long rngSeed = 42;

    /**
     * Set all fields to the values of the provided configuration. Useful for starting from some default.
     *
     * @param config
     * @return mutated version of the same builder instance
     */
    public static DataGeneratorConfigBuilder from( DataGeneratorConfig config )
    {
        DataGeneratorConfigBuilder builder = new DataGeneratorConfigBuilder();
        builder.nodeCount = config.nodeCount();
        builder.outRelationships = config.outRelationships();
        builder.relationshipOrder = config.relationshipOrder();
        builder.relationshipLocality = config.relationshipLocality();
        builder.graphWriter = config.graphWriter();
        builder.nodeProperties = config.nodeProperties();
        builder.relationshipProperties = config.relationshipProperties();
        builder.propertyLocality = config.propertyLocality();
        builder.propertyOrder = config.propertyOrder();
        builder.labels = config.labels();
        builder.labelOrder = config.labelOrder();
        builder.labelLocality = config.labelLocality();
        builder.schemaIndexes = config.schemaIndexes();
        builder.uniqueConstraints = config.uniqueConstraints();
        builder.mandatoryNodeConstraints = config.mandatoryNodeConstraints();
        builder.mandatoryRelationshipConstraints = config.mandatoryRelationshipConstraints();
        builder.fulltextNodeSchemaIndexes = config.fulltextNodeSchemaIndexes();
        builder.fulltextRelationshipSchemaIndexes = config.fulltextRelationshipSchemaIndexes();
        builder.neo4jConfig = config.neo4jConfig();
        builder.isReusable = config.isReusable();
        builder.augmentedBy = config.augmentedBy();
        builder.rngSeed = config.rngSeed();
        return builder;
    }

    /**
     * Number of nodes to be generated
     *
     * @param nodeCount
     * @return mutated version of the same builder instance
     */
    public DataGeneratorConfigBuilder withNodeCount( int nodeCount )
    {
        this.nodeCount = nodeCount;
        return this;
    }

    /**
     * Out going relationships for every node.
     * Nodes will likely have the same set of incoming relationships, but that is not guaranteed.
     * Moreover, Order & Locality only apply to outgoing relationships.
     *
     * @param outRelationships
     * @return mutated version of the same builder instance
     */
    public DataGeneratorConfigBuilder withOutRelationships( RelationshipDefinition... outRelationships )
    {
        this.outRelationships = outRelationships;
        return this;
    }

    /**
     * Order relationships are added for each individual node.
     * Intended outcome: order relationships appear in relationship chains.
     * <p>
     * {@link com.neo4j.bench.micro.data.DataGenerator.Order#ORDERED}: order in which relationships were specified in
     * {@link com.neo4j.bench.micro.data.DataGeneratorConfigBuilder#withOutRelationships(RelationshipDefinition...)}.
     * <p>
     * {@link com.neo4j.bench.micro.data.DataGenerator.Order#SHUFFLED}: shuffled for each node.
     *
     * @param relationshipOrder
     * @return mutated version of the same builder instance
     */
    public DataGeneratorConfigBuilder withRelationshipOrder( Order relationshipOrder )
    {
        this.relationshipOrder = relationshipOrder;
        return this;
    }

    /**
     * Order in which outgoing relationships are added to each node.
     * <p>
     * {@link com.neo4j.bench.micro.data.DataGenerator.RelationshipLocality#CO_LOCATED_BY_START_NODE}: all outgoing
     * relationships are added to a node consecutively. Once an outgoing relationship has been added to a node, no
     * outgoing relationships will be added to any other node until all of its outgoing relationships have been
     * added. The intention of this policy is that outgoing relationships for each node are spatially close to one
     * another in memory (in contiguous pages).
     * <p>
     * {@link com.neo4j.bench.micro.data.DataGenerator.RelationshipLocality#SCATTERED_BY_START_NODE}: outgoing
     * relationships are added to nodes in round-robin fashion. The intention of this policy is that outgoing
     * relationships for each node are spatially distant from one another in memory (scattered all over the store).
     * <p>
     * {@link com.neo4j.bench.micro.data.DataGenerator.RelationshipLocality} respects the
     * {@link com.neo4j.bench.micro.data.DataGenerator.Order} set in
     * {@link com.neo4j.bench.micro.data.DataGeneratorConfigBuilder#withRelationshipOrder(Order)}
     *
     * @param relationshipLocality
     * @return mutated version of the same builder instance
     */
    public DataGeneratorConfigBuilder withRelationshipLocality( RelationshipLocality relationshipLocality )
    {
        this.relationshipLocality = relationshipLocality;
        return this;
    }

    /**
     * Specifies which API is used to generate the store.
     * <p>
     * {@link com.neo4j.bench.micro.data.DataGenerator.GraphWriter#TRANSACTIONAL}: every write operation is performed
     * within a transaction.
     * <p>
     * {@link com.neo4j.bench.micro.data.DataGenerator.GraphWriter#BATCH}: write operations may be performed
     * non-transactionally (e.g., using batch inserter).
     * <p>
     * {@link com.neo4j.bench.micro.data.DataGenerator.GraphWriter} respects the value set in
     * {@link com.neo4j.bench.micro.data.DataGeneratorConfigBuilder#withGraphWriter(GraphWriter)}
     *
     * @param graphWriter
     * @return mutated version of the same builder instance
     */
    public DataGeneratorConfigBuilder withGraphWriter( GraphWriter graphWriter )
    {
        this.graphWriter = graphWriter;
        return this;
    }

    /**
     * Properties (key + value generator) to add to every node.
     * Every node will have the same set of properties keys.
     * Property values depend on the value generator specified.
     *
     * @param properties
     * @return mutated version of the same builder instance
     */
    public DataGeneratorConfigBuilder withNodeProperties( PropertyDefinition... properties )
    {
        Map<String,Long> propertyKeyCounts = Stream.of( properties )
                                                   .map( PropertyDefinition::key )
                                                   .collect( groupingBy( identity(), counting() ) );
        if ( propertyKeyCounts.size() != properties.length )
        {
            String incorrectCountsMsg = propertyKeyCounts.entrySet().stream()
                                                         .filter( e -> e.getValue() > 1 )
                                                         .map( e -> format( "Found %s entries of %s", e.getValue(), e.getKey() ) )
                                                         .collect( joining( "\n" ) );
            throw new IllegalArgumentException( "Duplicate node property keys\n" + incorrectCountsMsg );
        }
        this.nodeProperties = properties;
        return this;
    }

    /**
     * Properties (key + value generator) to add to every relationship.
     * Every relationship will have the same set of properties keys.
     * Property values depend on the value generator specified.
     *
     * @param properties
     * @return mutated version of the same builder instance
     */
    public DataGeneratorConfigBuilder withRelationshipProperties( PropertyDefinition... properties )
    {
        Map<String,Long> propertyKeyCounts = Stream.of( properties )
                                                   .map( PropertyDefinition::key )
                                                   .collect( groupingBy( identity(), counting() ) );
        if ( propertyKeyCounts.size() != properties.length )
        {
            String incorrectCountsMsg = propertyKeyCounts.entrySet().stream()
                                                         .filter( e -> e.getValue() > 1 )
                                                         .map( e -> format( "Found %s entries of %s", e.getValue(), e.getKey() ) )
                                                         .collect( joining( "\n" ) );
            throw new IllegalArgumentException( "Duplicate relationship property keys\n" + incorrectCountsMsg );
        }
        this.relationshipProperties = properties;
        return this;
    }

    /**
     * Order properties are added for each individual node/relationship.
     * Intended outcome: order properties appear in property chains.
     * <p>
     * {@link com.neo4j.bench.micro.data.DataGenerator.Order#ORDERED}: order in which properties were specified in
     * {@link com.neo4j.bench.micro.data.DataGeneratorConfigBuilder#withNodeProperties(PropertyDefinition...)} and
     * {@link com.neo4j.bench.micro.data.DataGeneratorConfigBuilder#withRelationshipProperties(PropertyDefinition...)}.
     * <p>
     * {@link com.neo4j.bench.micro.data.DataGenerator.Order#SHUFFLED}: shuffled for each node/relationship.
     *
     * @param propertyOrder
     * @return mutated version of the same builder instance
     */
    public DataGeneratorConfigBuilder withPropertyOrder( Order propertyOrder )
    {
        this.propertyOrder = propertyOrder;
        return this;
    }

    /**
     * Order in which properties are added to nodes/relationships.
     * <p>
     * {@link com.neo4j.bench.micro.data.DataGenerator.PropertyLocality#CO_LOCATED_BY_ELEMENT}: all properties are added to
     * a node/relationship consecutively. Once a property has been added to a node/relationship, no properties
     * will be added to any other node/relationship until all of its properties have been added.
     * The intention of this policy is that properties for each node/relationship are spatially close to one
     * another in memory (in contiguous pages).
     * <p>
     * {@link com.neo4j.bench.micro.data.DataGenerator.PropertyLocality#SCATTERED_BY_ELEMENT}: properties
     * are added to nodes/relationships in round-robin fashion. The intention of this policy is that properties
     * for each node/relationship are spatially distant from one another in memory (scattered all over the store).
     * <p>
     * {@link com.neo4j.bench.micro.data.DataGenerator.PropertyLocality} respects the
     * {@link com.neo4j.bench.micro.data.DataGenerator.Order} set in
     * {@link com.neo4j.bench.micro.data.DataGeneratorConfigBuilder#withPropertyOrder(Order)}
     *
     * @param propertyLocality
     * @return mutated version of the same builder instance
     */
    public DataGeneratorConfigBuilder withPropertyLocality( PropertyLocality propertyLocality )
    {
        this.propertyLocality = propertyLocality;
        return this;
    }

    /**
     * Labels to add to every node. Every node will have the same set of labels.
     *
     * @param labels
     * @return mutated version of the same builder instance
     */
    public DataGeneratorConfigBuilder withLabels( Label... labels )
    {
        this.labels = labels;
        return this;
    }

    /**
     * Order labels are added for each individual node.
     * <p>
     * {@link com.neo4j.bench.micro.data.DataGenerator.Order#ORDERED}: order in which labels were specified in
     * {@link com.neo4j.bench.micro.data.DataGeneratorConfigBuilder#withLabels(Label...)}.
     * <p>
     * {@link com.neo4j.bench.micro.data.DataGenerator.Order#SHUFFLED}: shuffled for each node.
     *
     * @param labelOrder
     * @return mutated version of the same builder instance
     */
    public DataGeneratorConfigBuilder withLabelOrder( Order labelOrder )
    {
        this.labelOrder = labelOrder;
        return this;
    }

    /**
     * Order in which labels are added to nodes/relationships.
     * <p>
     * {@link com.neo4j.bench.micro.data.DataGenerator.LabelLocality#CO_LOCATED_BY_NODE}: all labels are added to
     * a node consecutively. Once a label has been added to a node, no labels
     * will be added to any other node until all of its labels have been added.
     * The intention of this policy is that labels for each node are spatially close to one
     * another in memory (in contiguous pages).
     * <p>
     * {@link com.neo4j.bench.micro.data.DataGenerator.LabelLocality#SCATTERED_BY_NODE}: labels
     * are added to nodes in round-robin fashion. The intention of this policy is that labels
     * for each node are spatially distant from one another in memory (scattered all over the store).
     * <p>
     * {@link com.neo4j.bench.micro.data.DataGenerator.LabelLocality} respects the
     * {@link com.neo4j.bench.micro.data.DataGenerator.Order} set in
     * {@link com.neo4j.bench.micro.data.DataGeneratorConfigBuilder#withLabelOrder(Order)}
     *
     * @param labelLocality
     * @return mutated version of the same builder instance
     */
    public DataGeneratorConfigBuilder withLabelLocality( LabelLocality labelLocality )
    {
        this.labelLocality = labelLocality;
        return this;
    }

    /**
     * Label:property pairs to create schema indexes for.
     *
     * @param schemaIndexes
     * @return mutated version of the same builder instance
     */
    public DataGeneratorConfigBuilder withSchemaIndexes( LabelKeyDefinition... schemaIndexes )
    {
        this.schemaIndexes = schemaIndexes;
        return this;
    }

    /**
     * Label:property pairs to create unique constraints for.
     *
     * @param uniqueConstraints
     * @return mutated version of the same builder instance
     */
    public DataGeneratorConfigBuilder withUniqueConstraints( LabelKeyDefinition... uniqueConstraints )
    {
        this.uniqueConstraints = uniqueConstraints;
        return this;
    }

    /**
     * Label:property pairs to create mandatory constraints for.
     *
     * @param mandatoryNodeConstraints
     * @return mutated version of the same builder instance
     */
    public DataGeneratorConfigBuilder withMandatoryNodeConstraints( LabelKeyDefinition... mandatoryNodeConstraints )
    {
        this.mandatoryNodeConstraints = mandatoryNodeConstraints;
        return this;
    }

    /**
     * RelationshipType:property pairs to create mandatory constraints for.
     *
     * @param mandatoryRelationshipConstraints
     * @return mutated version of the same builder instance
     */
    public DataGeneratorConfigBuilder withMandatoryRelationshipConstraints(
            RelationshipKeyDefinition... mandatoryRelationshipConstraints )
    {
        this.mandatoryRelationshipConstraints = mandatoryRelationshipConstraints;
        return this;
    }

    /**
     * Label:property... pairs to create fulltext schema indexes for.
     *
     * @param fulltextNodeSchemaIndexes
     * @return mutated version of the same builder instance.
     */
    public DataGeneratorConfigBuilder withFulltextNodeSchemaIndexes( LabelKeyDefinition... fulltextNodeSchemaIndexes )
    {
        this.fulltextNodeSchemaIndexes = fulltextNodeSchemaIndexes;
        return this;
    }

    /**
     * RelationshipType:property... pairs to create fulltext relationship schema indexes for.
     *
     * @param fulltextRelationshipSchemaIndexes
     * @return mutated version of the same builder instance.
     */
    public DataGeneratorConfigBuilder withFulltextRelationshipSchemaIndexes( RelationshipKeyDefinition... fulltextRelationshipSchemaIndexes )
    {
        this.fulltextRelationshipSchemaIndexes = fulltextRelationshipSchemaIndexes;
        return this;
    }

    /**
     * Neo4j configuration to (1) use for store generation and (2) open the resulting store with when benchmarking.
     *
     * @param neo4jConfig
     * @return mutated version of the same builder instance
     */
    public DataGeneratorConfigBuilder withNeo4jConfig( Neo4jConfig neo4jConfig )
    {
        this.neo4jConfig = neo4jConfig;
        return this;
    }

    /**
     * If a store is reusable it will be generated exactly once, then never regenerated nor copied.
     * <p>
     * If a store is not reusable it will be generated exactly once, then a copy will be made for each benchmark that
     * requests. The store copies will always be deleted immediately after the associated benchmark completes. This
     * is done to (1) save disc space (2) avoid unnecessary store generation time.
     *
     * @param isReusable
     * @return mutated version of the same builder instance
     */
    public DataGeneratorConfigBuilder isReusableStore( boolean isReusable )
    {
        this.isReusable = isReusable;
        return this;
    }

    /**
     * If a store is augmented, determinism of store generation can not be trusted.
     * <p>
     * NOTE: Do not explicitly set this, if you do it will be overridden by the framework anyway.
     *
     * @param augmentedBy
     * @return mutated version of the same builder instance
     */
    public DataGeneratorConfigBuilder augmentedBy( String augmentedBy )
    {
        this.augmentedBy = augmentedBy;
        return this;
    }

    /**
     * Random seed to use during store generation.
     *
     * @param rngSeed
     * @return mutated version of the same builder instance
     */
    public DataGeneratorConfigBuilder withRngSeed( long rngSeed )
    {
        this.rngSeed = rngSeed;
        return this;
    }

    public DataGeneratorConfig build()
    {
        return new DataGeneratorConfig(
                nodeCount,
                outRelationships,
                relationshipOrder,
                relationshipLocality,
                graphWriter,
                nodeProperties,
                relationshipProperties,
                propertyLocality,
                propertyOrder,
                labels,
                labelOrder,
                labelLocality,
                schemaIndexes,
                uniqueConstraints,
                mandatoryNodeConstraints,
                mandatoryRelationshipConstraints,
                fulltextNodeSchemaIndexes,
                fulltextRelationshipSchemaIndexes,
                neo4jConfig,
                isReusable,
                augmentedBy,
                rngSeed );
    }
}
