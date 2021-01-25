/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.security.enterprise.auth;

import org.neo4j.internal.kernel.api.security.PrivilegeAction;
import org.neo4j.kernel.api.exceptions.InvalidArgumentsException;

import static org.neo4j.internal.kernel.api.security.PrivilegeAction.ADMIN;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.DATABASE_ACTIONS;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.EXECUTE;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.EXECUTE_ADMIN;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.EXECUTE_BOOSTED;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.GRAPH_ACTIONS;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.REMOVE_LABEL;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.SET_LABEL;

public interface Resource
{
    void assertValidCombination( PrivilegeAction action ) throws InvalidArgumentsException;

    Type type();

    class GraphResource implements Resource
    {
        @Override
        public void assertValidCombination( PrivilegeAction action ) throws InvalidArgumentsException
        {
            if ( SET_LABEL.satisfies( action ) || REMOVE_LABEL.satisfies( action ) || !(GRAPH_ACTIONS.satisfies( action )) )
            {
                throw new InvalidArgumentsException( String.format( "Graph resource cannot be combined with action '%s'", action.toString() ) );
            }
        }

        @Override
        public Type type()
        {
            return Type.GRAPH;
        }

        @Override
        public String toString()
        {
            return "";
        }

        @Override
        public int hashCode()
        {
            return Type.GRAPH.hashCode();
        }

        @Override
        public boolean equals( Object obj )
        {
            return obj instanceof GraphResource;
        }
    }

    class DatabaseResource implements Resource
    {
        @Override
        public void assertValidCombination( PrivilegeAction action ) throws InvalidArgumentsException
        {
            if ( !(ADMIN.satisfies( action ) ||
                   DATABASE_ACTIONS.satisfies( action ) ||
                   EXECUTE.satisfies( action ) ||
                   EXECUTE_BOOSTED.satisfies( action ) ||
                   EXECUTE_ADMIN.satisfies( action )) )
            {
                throw new InvalidArgumentsException( String.format( "Database resource cannot be combined with action '%s'", action.toString() ) );
            }
        }

        @Override
        public Type type()
        {
            return Type.DATABASE;
        }

        @Override
        public String toString()
        {
            return "";
        }

        @Override
        public int hashCode()
        {
            return Type.DATABASE.hashCode();
        }

        @Override
        public boolean equals( Object obj )
        {
            return obj instanceof DatabaseResource;
        }
    }

    class AllPropertiesResource implements Resource
    {
        public AllPropertiesResource( )
        {
        }

        @Override
        public void assertValidCombination( PrivilegeAction action ) throws InvalidArgumentsException
        {
            if ( !(action.equals( PrivilegeAction.READ ) || action.equals( PrivilegeAction.MATCH ) ||
                   action.equals( PrivilegeAction.SET_PROPERTY ) || action.equals( PrivilegeAction.MERGE )) )
            {
                throw new InvalidArgumentsException( String.format( "Property resource cannot be combined with action `%s`", action.toString() ) );
            }
        }

        @Override
        public Type type()
        {
            return Type.ALL_PROPERTIES;
        }

        @Override
        public String toString()
        {
            return "*";
        }

        @Override
        public int hashCode()
        {
            return Type.ALL_PROPERTIES.hashCode();
        }

        @Override
        public boolean equals( Object obj )
        {
            return obj == this || obj instanceof AllPropertiesResource;
        }
    }

    class PropertyResource implements Resource
    {
        private final String property;

        public PropertyResource( String property )
        {
            this.property = property;
        }

        @Override
        public void assertValidCombination( PrivilegeAction action ) throws InvalidArgumentsException
        {
            if ( !(action.equals( PrivilegeAction.READ ) || action.equals( PrivilegeAction.MATCH ) ||
                   action.equals( PrivilegeAction.SET_PROPERTY ) || action.equals( PrivilegeAction.MERGE )) )
            {
                throw new InvalidArgumentsException( String.format( "Property resource cannot be combined with action `%s`", action.toString() ) );
            }
        }

        @Override
        public Type type()
        {
            return Type.PROPERTY;
        }

        @Override
        public String toString()
        {
            return property;
        }

        @Override
        public int hashCode()
        {
            return property == null ? 0 : property.hashCode() + 31 * Type.PROPERTY.hashCode();
        }

        @Override
        public boolean equals( Object obj )
        {
            if ( obj == this )
            {
                return true;
            }

            if ( obj instanceof PropertyResource )
            {
                PropertyResource other = (PropertyResource) obj;
                return this.property == null && other.property == null || this.property != null && this.property.equals( other.property );
            }
            return false;
        }

        public String getProperty()
        {
            return property == null ? "" : property;
        }
    }

    class AllLabelsResource implements Resource
    {
        public AllLabelsResource( )
        {
        }

        @Override
        public void assertValidCombination( PrivilegeAction action ) throws InvalidArgumentsException
        {
            if ( !(action.equals( PrivilegeAction.SET_LABEL ) || action.equals( PrivilegeAction.REMOVE_LABEL )) )
            {
                throw new InvalidArgumentsException( String.format( "Label resource cannot be combined with action `%s`", action.toString() ) );
            }
        }

        @Override
        public Type type()
        {
            return Type.ALL_LABELS;
        }

        @Override
        public String toString()
        {
            return "*";
        }

        @Override
        public int hashCode()
        {
            return Type.ALL_LABELS.hashCode();
        }

        @Override
        public boolean equals( Object obj )
        {
            return obj == this || obj instanceof AllLabelsResource;
        }
    }

    class LabelResource implements Resource
    {
        private final String label;

        public LabelResource( String label )
        {
            this.label = label;
        }

        @Override
        public void assertValidCombination( PrivilegeAction action ) throws InvalidArgumentsException
        {
            if ( !(action.equals( PrivilegeAction.SET_LABEL ) || action.equals( PrivilegeAction.REMOVE_LABEL )) )
            {
                throw new InvalidArgumentsException( String.format( "Label resource cannot be combined with action `%s`", action.toString() ) );
            }
        }

        @Override
        public Type type()
        {
            return Type.LABEL;
        }

        @Override
        public String toString()
        {
            return label;
        }

        @Override
        public int hashCode()
        {
            return label == null ? 0 : label.hashCode() + 31 * Type.LABEL.hashCode();
        }

        @Override
        public boolean equals( Object obj )
        {
            if ( obj == this )
            {
                return true;
            }

            if ( obj instanceof LabelResource )
            {
                LabelResource other = (LabelResource) obj;
                return this.label == null && other.label == null || this.label != null && this.label.equals( other.label );
            }
            return false;
        }

        public String getLabel()
        {
            return label == null ? "" : label;
        }
    }

    enum Type
    {
        ALL_PROPERTIES,
        PROPERTY,
        ALL_LABELS,
        LABEL,
        GRAPH,
        DATABASE;

        @Override
        public String toString()
        {
            return super.toString().toLowerCase();
        }
    }
}
