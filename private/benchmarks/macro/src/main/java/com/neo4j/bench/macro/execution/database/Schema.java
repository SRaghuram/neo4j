/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.macro.execution.database;

import com.neo4j.bench.macro.workload.WorkloadConfigError;
import com.neo4j.bench.macro.workload.WorkloadConfigException;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.schema.IndexCreator;

import static java.lang.String.format;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;

public class Schema
{
    static void assertEqual( Schema expectedSchema, Schema actualSchema )
    {
        if ( !expectedSchema.equals( actualSchema ) )
        {
            List<SchemaEntry> missingFromActual = expectedSchema.entriesNotIn( actualSchema );
            List<SchemaEntry> extraInActual = actualSchema.entriesNotIn( expectedSchema );
            String errorMessage = String.format( "Actual schema did not equal expected schema!\n" +
                                                 "The following entries were missing from actual schema:\n" +
                                                 "%s\n" +
                                                 "The following entries were unexpectedly found in actual schema:\n" +
                                                 "%s",
                                                 missingFromActual.stream()
                                                                  .map( SchemaEntry::description )
                                                                  .map( desc -> "  * " + desc )
                                                                  .collect( joining( "\n" ) ),
                                                 extraInActual.stream()
                                                              .map( SchemaEntry::description )
                                                              .map( desc -> "  * " + desc )
                                                              .collect( joining( "\n" ) ) );
            throw new RuntimeException( errorMessage );
        }
    }

    public static Schema loadFrom( List<String> schemaDescriptions )
    {
        List<SchemaEntry> schemaEntries = schemaDescriptions.stream()
                                                            .map( SchemaEntry::parse )
                                                            .collect( toList() );
        return new Schema( schemaEntries );
    }

    private final List<SchemaEntry> entries;

    Schema( List<SchemaEntry> entries )
    {
        this.entries = entries;
    }

    public Optional<List<SchemaEntry>> duplicates()
    {
        List<SchemaEntry> duplicates = entries.stream()
                                              .collect( groupingBy( SchemaEntry::description ) )
                                              .entrySet().stream()
                                              .filter( e -> e.getValue().size() > 1 )
                                              .map( e -> e.getValue().get( 0 ) )
                                              .collect( toList() );
        return duplicates.isEmpty() ? Optional.empty() : Optional.of( duplicates );
    }

    public boolean isEmpty()
    {
        return entries.isEmpty();
    }

    List<SchemaEntry> constraints()
    {
        return entries.stream().filter( entry -> !(entry instanceof IndexSchemaEntry || entry instanceof RelationshipIndexSchemaEntry) ).collect( toList() );
    }

    List<SchemaEntry> indexes()
    {
        return entries.stream().filter( entry -> entry instanceof IndexSchemaEntry ).collect( toList() );
    }

    List<RelationshipIndexSchemaEntry> relationshipIndexes()
    {
        return entries.stream().filter( entry -> entry instanceof RelationshipIndexSchemaEntry )
                      .map( r -> (RelationshipIndexSchemaEntry) r )
                      .collect( toList() );
    }

    private List<SchemaEntry> entriesNotIn( Schema schema )
    {
        return entries.stream()
                      .filter( schemaEntry -> !schema.contains( schemaEntry ) )
                      .collect( toList() );
    }

    public boolean contains( SchemaEntry entry )
    {
        return entries.contains( entry );
    }

    @Override
    public String toString()
    {
        String constraints = constraints().stream()
                                          .map( SchemaEntry::description )
                                          .sorted()
                                          .collect( joining( "\n" ) );
        String indexes = indexes().stream()
                                  .map( SchemaEntry::description )
                                  .sorted()
                                  .collect( joining( "\n" ) );
        return constraints + "\n" + indexes;
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
        Schema schema = (Schema) o;
        // order is not important
        return entries.containsAll( schema.entries ) &&
               schema.entries.containsAll( entries );
    }

    @Override
    public int hashCode()
    {
        return HashCodeBuilder.reflectionHashCode( this );
    }

    public interface SchemaEntry
    {
        String description();

        String createStatement();

        static SchemaEntry parse( String value )
        {
            if ( NodeKeySchemaEntry.isNodeKey( value ) )
            {
                return NodeKeySchemaEntry.parse( value );
            }
            if ( NodeUniqueSchemaEntry.isNodeUnique( value ) )
            {
                return NodeUniqueSchemaEntry.parse( value );
            }
            else if ( NodeExistsSchemaEntry.isNodeExists( value ) )
            {
                return NodeExistsSchemaEntry.parse( value );
            }
            else if ( RelationshipExistsSchemaEntry.isRelationshipExists( value ) )
            {
                return RelationshipExistsSchemaEntry.parse( value );
            }
            else if ( IndexSchemaEntry.isIndex( value ) )
            {
                return IndexSchemaEntry.parse( value );
            }
            else if ( RelationshipIndexSchemaEntry.isIndex( value ) )
            {
                return RelationshipIndexSchemaEntry.parse( value );
            }
            else
            {
                throw new WorkloadConfigException( "Unrecognized schema entry: " + value, WorkloadConfigError.INVALID_SCHEMA_ENTRY );
            }
        }
    }

    // INDEX ON :Label(prop1,prop2)
    static class IndexSchemaEntry implements SchemaEntry
    {
        static boolean isIndex( String value )
        {
            return value.startsWith( "INDEX ON :" );
        }

        static SchemaEntry parse( String value )
        {
            if ( !isIndex( value ) )
            {
                throw new WorkloadConfigException( WorkloadConfigError.INVALID_SCHEMA_ENTRY );
            }
            // [ Label , 'prop1,prop2' ]
            String[] elements = value
                    // Label(author
                    .replace( ")", "" )
                    .replace( ":", "" )
                    .replace( "INDEX ON", "" )
                    // [ Label , 'prop1,prop2' ]
                    .split( "\\(" );

            // [ Label , 'prop1,prop2' ] --> Label
            String label = elements[0].trim();
            // [ Label , 'prop1,prop2' ] --> 'prop1,prop2'
            List<String> properties = Arrays.stream( elements[1].trim().split( "," ) ).map( String::trim ).collect( toList() );

            return new IndexSchemaEntry( Label.label( label ), properties );
        }

        private final Label label;
        private final List<String> properties;

        IndexSchemaEntry( Label label, List<String> properties )
        {
            this.label = label;
            this.properties = properties;
        }

        @Override
        public String createStatement()
        {
            return "CREATE " + description();
        }

        @Override
        public String description()
        {
            return "INDEX ON :" + label.name() + "(" + String.join( ",", properties ) + ")";
        }

        Label label()
        {
            return label;
        }

        List<String> properties()
        {
            return properties;
        }

        @Override
        public boolean equals( Object o )
        {
            return EqualsBuilder.reflectionEquals( this, o );
        }

        @Override
        public int hashCode()
        {
            return HashCodeBuilder.reflectionHashCode( this );
        }
    }

    // CONSTRAINT ON ( n:Label ) ASSERT exists(n.prop)
    static class NodeExistsSchemaEntry implements SchemaEntry
    {
        static boolean isNodeExists( String value )
        {
            return value.startsWith( "CONSTRAINT ON (" ) && value.contains( ") ASSERT exists(" ) && !value.contains( "()-[" ) && !value.contains( "]-()" );
        }

        static SchemaEntry parse( String value )
        {
            if ( !isNodeExists( value ) )
            {
                throw new WorkloadConfigException( WorkloadConfigError.INVALID_SCHEMA_ENTRY );
            }
            // ['n:Label   ',' n.prop']
            String[] elements = value
                    // CONSTRAINT ON   n:Label   ASSERT exists n.prop
                    .replace( ")", " " )
                    .replace( "(", " " )
                    // n:Label   ASSERT exists n.prop
                    .replace( "CONSTRAINT ON", "" )
                    // ['n:Label   ',' n.prop']
                    .split( "ASSERT exists" );

            // n:Label --> [ n , Label ]
            String label = elements[0].split( ":" )[1].trim();
            // n.prop --> [ n , prop ]
            String property = elements[1].split( "\\." )[1].trim();

            return new NodeExistsSchemaEntry( Label.label( label ), property );
        }

        private final Label label;
        private final String property;

        NodeExistsSchemaEntry( Label label, String property )
        {
            this.label = label;
            this.property = property;
        }

        @Override
        public String createStatement()
        {
            return "CREATE " + description();
        }

        @Override
        public String description()
        {
            return "CONSTRAINT ON (n:" + label.name() + ") ASSERT exists(n." + property + ")";
        }

        Label label()
        {
            return label;
        }

        String property()
        {
            return property;
        }

        @Override
        public boolean equals( Object o )
        {
            return EqualsBuilder.reflectionEquals( this, o );
        }

        @Override
        public int hashCode()
        {
            return HashCodeBuilder.reflectionHashCode( this );
        }
    }

    // CONSTRAINT ON ()-[r:TYPE]-() ASSERT exists(r.prop)
    static class RelationshipExistsSchemaEntry implements SchemaEntry
    {
        static boolean isRelationshipExists( String value )
        {
            return value.startsWith( "CONSTRAINT ON ()-[" ) && value.contains( " ASSERT exists(" );
        }

        static SchemaEntry parse( String value )
        {
            if ( !isRelationshipExists( value ) )
            {
                throw new WorkloadConfigException( WorkloadConfigError.INVALID_SCHEMA_ENTRY );
            }
            // [ r:TYPE , r.prop ]
            String[] elements = value
                    // CONSTRAINT ON     r:TYPE     ASSERT exists r.prop
                    .replace( "(", " " )
                    .replace( ")", " " )
                    .replace( "[", "" )
                    .replace( "]", "" )
                    .replace( "-", "" )
                    //      r:TYPE     ASSERT exists r.prop
                    .replace( "CONSTRAINT ON", "" )
                    // [ r:TYPE , r.prop ]
                    .split( "ASSERT exists" );

            // r:TYPE --> [ r , TYPE ]
            String type = elements[0].split( ":" )[1].trim();
            // r.prop --> [ r , prop ]
            String property = elements[1].split( "\\." )[1].trim();

            return new RelationshipExistsSchemaEntry( RelationshipType.withName( type ), property );
        }

        private final RelationshipType type;
        private final String property;

        RelationshipExistsSchemaEntry( RelationshipType type, String property )
        {
            this.type = type;
            this.property = property;
        }

        @Override
        public String createStatement()
        {
            return "CREATE " + description();
        }

        @Override
        public String description()
        {
            return "CONSTRAINT ON ()-[r:" + type.name() + "]-() ASSERT exists(r." + property + ")";
        }

        RelationshipType type()
        {
            return type;
        }

        String property()
        {
            return property;
        }

        @Override
        public boolean equals( Object o )
        {
            return EqualsBuilder.reflectionEquals( this, o );
        }

        @Override
        public int hashCode()
        {
            return HashCodeBuilder.reflectionHashCode( this );
        }
    }

    // CONSTRAINT ON ( n:Label ) ASSERT n.prop IS UNIQUE
    static class NodeUniqueSchemaEntry implements SchemaEntry
    {
        static boolean isNodeUnique( String value )
        {
            return value.startsWith( "CONSTRAINT ON " ) && value.endsWith( " IS UNIQUE" );
        }

        static SchemaEntry parse( String value )
        {
            if ( !isNodeUnique( value ) )
            {
                throw new WorkloadConfigException( WorkloadConfigError.INVALID_SCHEMA_ENTRY );
            }
            // [ n:Label , n.prop ]
            String[] elements = value
                    // n:Label   ASSERT n.prop
                    .replace( "(", " " )
                    .replace( ")", " " )
                    .replace( "CONSTRAINT ON", "" )
                    .replace( " IS UNIQUE", "" )
                    // [ n:Label , n.prop ]
                    .split( "ASSERT" );

            // n:Label --> [ n , Label ]
            String label = elements[0].split( ":" )[1].trim();
            // n.prop --> [ n , prop ]
            String property = elements[1].split( "\\." )[1].trim();

            return new NodeUniqueSchemaEntry( Label.label( label ), property );
        }

        private final Label label;
        private final String property;

        NodeUniqueSchemaEntry( Label label, String property )
        {
            this.label = label;
            this.property = property;
        }

        @Override
        public String createStatement()
        {
            return "CREATE " + description();
        }

        @Override
        public String description()
        {
            return "CONSTRAINT ON (n:" + label.name() + ") ASSERT n." + property + " IS UNIQUE";
        }

        Label label()
        {
            return label;
        }

        String property()
        {
            return property;
        }

        @Override
        public boolean equals( Object o )
        {
            return EqualsBuilder.reflectionEquals( this, o );
        }

        @Override
        public int hashCode()
        {
            return HashCodeBuilder.reflectionHashCode( this );
        }
    }

    // CONSTRAINT ON (p:Person) ASSERT (p.prop1, p.prop2) IS NODE KEY
    static class NodeKeySchemaEntry implements SchemaEntry
    {
        static boolean isNodeKey( String value )
        {
            return value.startsWith( "CONSTRAINT ON (" ) && value.endsWith( ") IS NODE KEY" );
        }

        static SchemaEntry parse( String value )
        {
            if ( !isNodeKey( value ) )
            {
                throw new WorkloadConfigException( WorkloadConfigError.INVALID_SCHEMA_ENTRY );
            }
            // [ 'p:Label' , 'p.prop1, p.prop2' ]
            String[] elements = value
                    //   p:Person  ASSERT  p.prop1, p.prop2
                    .replace( "(", " " )
                    .replace( ")", " " )
                    .replace( "CONSTRAINT ON", "" )
                    .replace( "IS NODE KEY", "" )
                    // [ 'p:Label' , 'p.prop1, p.prop2' ]
                    .split( "ASSERT" );

            // 'n:Label' --> [ n , Label ]
            String label = elements[0].split( ":" )[1].trim();
            // 'p.prop1, p.prop2' --> [ 'n.prop1' , 'n.prop2' ]
            List<String> nodeProperties = Arrays.stream( elements[1].split( "," ) ).map( String::trim ).collect( toList() );
            // [ 'n.prop1' , 'n.prop2' ] --> [ 'prop1' , 'prop2' ]
            List<String> properties = nodeProperties.stream().map( nodeProp -> nodeProp.split( "\\." )[1].trim() ).collect( toList() );
            return new NodeKeySchemaEntry( Label.label( label ), properties );
        }

        private final Label label;
        private final List<String> properties;

        NodeKeySchemaEntry( Label label, List<String> properties )
        {
            this.label = label;
            this.properties = properties;
        }

        @Override
        public String createStatement()
        {
            return "CREATE " + description();
        }

        @Override
        public String description()
        {
            String props = properties.stream().map( prop -> "n." + prop ).collect( joining( "," ) );
            return "CONSTRAINT ON (n:" + label.name() + ") ASSERT (" + props + ") IS NODE KEY";
        }

        Label label()
        {
            return label;
        }

        List<String> properties()
        {
            return properties;
        }

        @Override
        public boolean equals( Object o )
        {
            return EqualsBuilder.reflectionEquals( this, o );
        }

        @Override
        public int hashCode()
        {
            return HashCodeBuilder.reflectionHashCode( this );
        }
    }

    //INDEX FOR ()-[r:Foo]->() ON (r.prop1, r.prop2)
    static class RelationshipIndexSchemaEntry implements SchemaEntry
    {
        private static final String regex = ".+\\[.+:(.+)].+ ON \\((.+)\\)";

        static boolean isIndex( String value )
        {
            return matchIndex( value ).matches();
        }

        static SchemaEntry parse( String value )
        {
            if ( !isIndex( value ) )
            {
                throw new WorkloadConfigException( WorkloadConfigError.INVALID_SCHEMA_ENTRY );
            }

            final Matcher matcher = matchIndex( value );
            if ( !matcher.matches() )
            {
                throw new RuntimeException( format( "Failed to parse index with value '%s'", value ) );
            }
            //Foo
            String relationshipType = matcher.group( 1 );

            // r.prop1, r.prop2 --> [ "r.prop1", "r.prop2" ]
            List<String> relationshipProperties = Arrays.stream( matcher.group( 2 ).split( "," ) ).map( String::trim ).collect( toList() );
            //[ "r.prop1", "r.prop2" ] -> [ "prop1", "prop2" ]
            List<String> properties = relationshipProperties.stream().map( relProp -> relProp.split( "\\." )[1].trim() ).collect( toList() );

            return new RelationshipIndexSchemaEntry( RelationshipType.withName( relationshipType ), properties );
        }

        private static Matcher matchIndex( String value )
        {
            final Pattern pattern = Pattern.compile( regex );
            final Matcher matcher = pattern.matcher( value );
            return matcher;
        }

        private final RelationshipType relationshipType;
        private final List<String> properties;

        RelationshipIndexSchemaEntry( RelationshipType RelationshipType, List<String> properties )
        {
            this.relationshipType = RelationshipType;
            this.properties = properties;
        }

        @Override
        public String createStatement()
        {
            return "CREATE " + description();
        }

        @Override
        public String description()
        {
            String props = properties.stream().map( prop -> "r." + prop ).collect( joining( "," ) );
            return "INDEX FOR ()-[r:" + relationshipType.name() + "]->() ON (" + props + ")";
        }

        RelationshipType relationshipType()
        {
            return relationshipType;
        }

        List<String> properties()
        {
            return properties;
        }

        @Override
        public boolean equals( Object o )
        {
            return EqualsBuilder.reflectionEquals( this, o );
        }

        @Override
        public int hashCode()
        {
            return HashCodeBuilder.reflectionHashCode( this );
        }
    }
}
