/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.security.enterprise.auth;

import org.neo4j.internal.kernel.api.security.PrivilegeAction;
import org.neo4j.kernel.api.exceptions.InvalidArgumentsException;

import static org.neo4j.internal.kernel.api.security.PrivilegeAction.ADMIN;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.DATABASE_ACTIONS;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.GRAPH_ACTIONS;

public interface Resource
{
    void assertValidCombination( PrivilegeAction action ) throws InvalidArgumentsException;

    default String getArg1()
    {
        return "";
    }

    default String getArg2()
    {
        return "";
    }

    Type type();

    class GraphResource implements Resource
    {
        @Override
        public void assertValidCombination( PrivilegeAction action ) throws InvalidArgumentsException
        {
            if ( !(GRAPH_ACTIONS.satisfies( action )) )
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
            return "graph";
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
            if ( !(ADMIN.satisfies( action ) || DATABASE_ACTIONS.satisfies( action )) )
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
            return "database";
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
            if ( !(action.equals( PrivilegeAction.READ ) || action.equals( PrivilegeAction.MATCH )) )
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
            return "all properties ";
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
            if ( !(action.equals( PrivilegeAction.READ ) || action.equals( PrivilegeAction.MATCH )) )
            {
                throw new InvalidArgumentsException( String.format( "Property resource cannot be combined with action `%s`", action.toString() ) );
            }
        }

        @Override
        public String getArg1()
        {
            return property == null ? "" : property;
        }

        @Override
        public Type type()
        {
            return Type.PROPERTY;
        }

        @Override
        public String toString()
        {
            return "property " + property;
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
            return "all labels ";
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
        public String getArg1()
        {
            return label == null ? "" : label;
        }

        @Override
        public Type type()
        {
            return Type.LABEL;
        }

        @Override
        public String toString()
        {
            return "label " + label;
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
    }

    class ProcedureResource implements Resource
    {
        private final String nameSpace;
        private final String procedure;

        public ProcedureResource( String nameSpace, String procedure )
        {
            this.nameSpace = nameSpace;
            this.procedure = procedure;
        }

        @Override
        public void assertValidCombination( PrivilegeAction action ) throws InvalidArgumentsException
        {
            if ( !action.equals( PrivilegeAction.EXECUTE ) )
            {
                throw new InvalidArgumentsException( String.format( "Procedure resource cannot be combined with action '%s'", action.toString() ) );
            }
        }

        @Override
        public String getArg1()
        {
            return nameSpace == null ? "" : nameSpace;
        }

        @Override
        public String getArg2()
        {
            return procedure == null ? "" : procedure;
        }

        @Override
        public Type type()
        {
            return Type.PROCEDURE;
        }

        @Override
        public String toString()
        {
            return "library " + nameSpace + " procedure " + procedure;
        }

        @Override
        public int hashCode()
        {
            int hash = nameSpace == null ? 0 : nameSpace.hashCode();
            return hash + 31 * (procedure == null ? 0 : procedure.hashCode());
        }

        @Override
        public boolean equals( Object obj )
        {
            if ( obj == this )
            {
                return true;
            }

            if ( obj instanceof ProcedureResource )
            {
                ProcedureResource other = (ProcedureResource) obj;
                boolean equals = other.nameSpace == null && this.nameSpace == null || other.nameSpace != null && other.nameSpace.equals( this.nameSpace );
                return equals && (other.procedure == null && this.procedure == null || other.procedure != null && other.procedure.equals( this.procedure ));
            }
            return false;
        }
    }

    enum Type
    {
        ALL_PROPERTIES,
        PROPERTY,
        ALL_LABELS,
        LABEL,
        GRAPH,
        DATABASE,
        PROCEDURE;

        @Override
        public String toString()
        {
            return super.toString().toLowerCase();
        }
    }
}
