/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.tools.applytx;

import com.neo4j.tools.input.Command;
import com.neo4j.tools.input.ConsoleInput;
import com.neo4j.tools.input.ConsoleUtil;
import io.airlift.airline.Arguments;
import io.airlift.airline.Cli;
import io.airlift.airline.Cli.CliBuilder;

import java.io.PrintStream;
import java.util.function.Supplier;

import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.RecordStore;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.PropertyRecord;
import org.neo4j.kernel.impl.store.record.Record;
import org.neo4j.kernel.impl.store.record.RelationshipGroupRecord;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.token.api.NamedToken;

import static org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer.NULL;
import static org.neo4j.kernel.impl.store.record.RecordLoad.NORMAL;

/**
 * Able to dump records and record chains. Works as a {@link ConsoleInput} {@link Command}.
 */
public class DumpRecordsCommand implements Command
{
    public static final String NAME = "dump";

    private interface Action
    {
        void run( NeoStores stores, PrintStream out );
    }

    private final Cli<Action> cli;
    private final Supplier<NeoStores> store;

    public DumpRecordsCommand( Supplier<NeoStores> store )
    {
        this.store = store;
        CliBuilder<Action> builder = Cli.<Action>builder( NAME )
                .withDescription( "Dump record contents" )
                .withCommands( DumpRelationshipTypes.class, Help.class )
                .withDefaultCommand( Help.class );
        builder.withGroup( "node" )
                .withCommands( DumpNodePropertyChain.class, DumpNodeRelationshipChain.class, Help.class )
                .withDefaultCommand( Help.class );
        builder.withGroup( "relationship" )
                .withCommands( DumpRelationshipPropertyChain.class, Help.class )
                .withDefaultCommand( Help.class );
        builder.withGroup( "tokens" )
                .withCommands( DumpRelationshipTypes.class, DumpLabels.class, DumpPropertyKeys.class, Help.class )
                .withDefaultCommand( Help.class );
        this.cli = builder.build();
    }

    @Override
    public void run( String[] args, PrintStream out )
    {
        cli.parse( args ).run( store.get(), out );
    }

    @Override
    public String toString()
    {
        return ConsoleUtil.airlineHelp( cli );
    }

    abstract static class DumpPropertyChain implements Action
    {
        @Arguments( title = "id", description = "Entity id", required = true )
        public long id;

        protected abstract long firstPropId( NeoStores stores );

        @Override
        public void run( NeoStores stores, PrintStream out )
        {
            long propId = firstPropId( stores );
            RecordStore<PropertyRecord> propertyStore = stores.getPropertyStore();
            PropertyRecord record = propertyStore.newRecord();
            while ( propId != Record.NO_NEXT_PROPERTY.intValue() )
            {
                propertyStore.getRecord( propId, record, NORMAL, NULL );
                // We rely on this method having the side-effect of loading the property blocks:
                record.numberOfProperties();
                out.println( record );
                propId = record.getNextProp();
            }
        }
    }

    @io.airlift.airline.Command( name = "properties", description = "Dump property chain for a node" )
    public static class DumpNodePropertyChain extends DumpPropertyChain
    {
        @Override
        protected long firstPropId( NeoStores stores )
        {
            RecordStore<NodeRecord> nodeStore = stores.getNodeStore();
            return nodeStore.getRecord( id, nodeStore.newRecord(), NORMAL, NULL ).getNextProp();
        }
    }

    @io.airlift.airline.Command( name = "properties", description = "Dump property chain for a relationship" )
    public static class DumpRelationshipPropertyChain extends DumpPropertyChain
    {
        @Override
        protected long firstPropId( NeoStores stores )
        {
            RecordStore<RelationshipRecord> relationshipStore = stores.getRelationshipStore();
            return relationshipStore.getRecord( id, relationshipStore.newRecord(), NORMAL, NULL ).getNextProp();
        }
    }

    @io.airlift.airline.Command( name = "relationships", description = "Dump relationship chain for a node" )
    public static class DumpNodeRelationshipChain implements Action
    {
        @Arguments( description = "Node id", required = true )
        public long id;

        @Override
        public void run( NeoStores stores, PrintStream out )
        {
            RecordStore<NodeRecord> nodeStore = stores.getNodeStore();
            NodeRecord node = nodeStore.getRecord( id, nodeStore.newRecord(), NORMAL, NULL );
            if ( node.isDense() )
            {
                RecordStore<RelationshipGroupRecord> relationshipGroupStore = stores.getRelationshipGroupStore();
                RelationshipGroupRecord group = relationshipGroupStore.newRecord();
                relationshipGroupStore.getRecord( node.getNextRel(), group, NORMAL, NULL );
                do
                {
                    out.println( "group " + group );
                    out.println( "out:" );
                    printRelChain( stores, out, group.getFirstOut() );
                    out.println( "in:" );
                    printRelChain( stores, out, group.getFirstIn() );
                    out.println( "loop:" );
                    printRelChain( stores, out, group.getFirstLoop() );
                    group = group.getNext() != -1 ?
                            relationshipGroupStore.getRecord( group.getNext(), group, NORMAL, NULL ) : null;
                } while ( group != null );
            }
            else
            {
                printRelChain( stores, out, node.getNextRel() );
            }
        }

        private void printRelChain( NeoStores stores, PrintStream out, long firstRelId )
        {
            for ( long rel = firstRelId; rel != Record.NO_NEXT_RELATIONSHIP.intValue(); )
            {
                RecordStore<RelationshipRecord> relationshipStore = stores.getRelationshipStore();
                RelationshipRecord record = relationshipStore.getRecord( rel, relationshipStore.newRecord(), NORMAL, NULL );
                out.println( rel + "\t" + record );
                if ( record.getFirstNode() == id )
                {
                    rel = record.getFirstNextRel();
                }
                else
                {
                    rel = record.getSecondNextRel();
                }
            }
        }
    }

    @io.airlift.airline.Command( name = "relationship-type", description = "Dump relationship type tokens" )
    public static class DumpRelationshipTypes implements Action
    {
        @Override
        public void run( NeoStores stores, PrintStream out )
        {
            for ( NamedToken token : stores.getRelationshipTypeTokenStore().getTokens( NULL ) )
            {
                out.println( token );
            }
        }
    }

    @io.airlift.airline.Command( name = "label", description = "Dump label tokens" )
    public static class DumpLabels implements Action
    {
        @Override
        public void run( NeoStores stores, PrintStream out )
        {
            for ( NamedToken token : stores.getLabelTokenStore().getTokens( NULL ) )
            {
                out.println( token );
            }
        }
    }

    @io.airlift.airline.Command( name = "property-key", description = "Dump property key tokens" )
    public static class DumpPropertyKeys implements Action
    {
        @Override
        public void run( NeoStores stores, PrintStream out )
        {
            for ( NamedToken token : stores.getPropertyKeyTokenStore().getTokens( NULL ) )
            {
                out.println( token );
            }
        }
    }

    public static class Help extends io.airlift.airline.Help implements Action
    {
        @Override
        public void run( NeoStores stores, PrintStream out )
        {
            run();
        }
    }
}
