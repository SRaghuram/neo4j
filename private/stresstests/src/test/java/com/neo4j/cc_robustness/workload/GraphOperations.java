/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.cc_robustness.workload;

import com.neo4j.cc_robustness.Orchestrator;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.reflect.Array;
import java.time.Duration;
import java.time.Period;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.neo4j.graphdb.ConstraintViolationException;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Lock;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.values.storable.CoordinateReferenceSystem;
import org.neo4j.values.storable.DateTimeValue;
import org.neo4j.values.storable.DateValue;
import org.neo4j.values.storable.DurationValue;
import org.neo4j.values.storable.LocalDateTimeValue;
import org.neo4j.values.storable.LocalTimeValue;
import org.neo4j.values.storable.PointValue;
import org.neo4j.values.storable.TimeValue;
import org.neo4j.values.storable.Values;

import static com.neo4j.cc_robustness.ReferenceNode.REFERENCE_NODE_LABEL;
import static java.time.temporal.ChronoUnit.SECONDS;
import static org.neo4j.graphdb.Label.label;
import static org.neo4j.graphdb.traversal.Uniqueness.RELATIONSHIP_PATH;
import static org.neo4j.internal.helpers.collection.Iterables.asList;
import static org.neo4j.internal.helpers.collection.Iterables.count;
import static org.neo4j.internal.helpers.collection.Iterables.filter;

/**
 * Naive set of operations. The data structure is a tree with a couple of children
 * per node and level. The tree can be extended and pruned by some operations,
 * and also traversed. Root node in the tree is the reference node of the db.
 */
public class GraphOperations
{
    private static final int MAX_STRING_LENGTH = 40;
    private static final int PROPERTIES_COUNT = 120;
    private static final int LABELS_COUNT = 70;
    private static final boolean ENABLE_SPATIAL = true;
    private static final boolean ENABLE_SPATIAL_3D = true;
    private static final boolean ENABLE_TEMPORAL = true;
    private static final boolean ENABLE_NEW_ARRAYS = true;
    private static final Random RANDOM = new Random();
    private static final AtomicInteger tx = new AtomicInteger();
    private static final CoordinateReferenceSystem[] KnownCRS =
            new CoordinateReferenceSystem[]{CoordinateReferenceSystem.WGS84, CoordinateReferenceSystem.Cartesian, CoordinateReferenceSystem.WGS84_3D,
                    CoordinateReferenceSystem.Cartesian_3D};
    private static final Predicate<IndexDefinition> NOT_CONSTRAINT = item -> !item.isConstraintIndex();
    private static List<TypeWrapper> primitives = Arrays.stream( TYPES.values() ).filter( t -> t.enable ).map( t -> (TypeWrapper) t::random ).collect(
            Collectors.toList() );
    private static List<TypeWrapper> arrays = Arrays
            .stream( TYPES.values() )
            .filter( t -> t.enable && t.enableArrays )
            .map( t -> (TypeWrapper) t::array )
            .collect( Collectors.toList() );
    private static String[] AVAILABLE_TIMEZONES = ZoneId.getAvailableZoneIds().toArray( new String[0] );
    private final GraphDatabaseService db;
    private final int maxDepth;
    private final ReferenceNodeStrategy referenceNodeStrategy;
    private final boolean acquireReadLocks;

    public GraphOperations( GraphDatabaseService db, int maxDepth, ReferenceNodeStrategy referenceNodeStrategy, boolean acquireReadLocks )
    {
        this.db = db;
        this.maxDepth = maxDepth;
        this.referenceNodeStrategy = referenceNodeStrategy;
        this.acquireReadLocks = acquireReadLocks;
    }

    private static Node findRandomLeafNode( Transaction tx, Node startingFromNode )
    {
        Node node = startingFromNode;
        while ( true )
        {
            Lock tentativeLock = tx.acquireWriteLock( node );
            if ( !node.hasRelationship( Direction.OUTGOING ) )
            {   // Return the leaf node, still locked
                return node;
            }

            // Release the tentative lock and look further
            tentativeLock.release();
            List<Relationship> allRelationships = asList( node.getRelationships( Direction.OUTGOING ) );
            Relationship rel = allRelationships.get( RANDOM.nextInt( allRelationships.size() ) );

            node = rel.getEndNode();
        }
    }

    private static Relationship findRandomRelationship( Node node )
    {
        List<Relationship> rels = new ArrayList<>();
        for ( Relationship rel : node.getRelationships( RelType.EXTRA ) )
        {
            rels.add( rel );
        }
        return rels.isEmpty() ? null : rels.get( (int) (Math.random() * rels.size()) );
    }

    private static void deleteRelationship( Relationship rel )
    {
        rel.delete();
    }

    private static boolean isAReferenceNode( Node node )
    {
        // There are different reference node strategies, depending on which type of graph operatioons
        // what they all have in common is that they lack INCOMING relationships.
        return !node.hasRelationship( Direction.INCOMING );
    }

    @SuppressWarnings( "unused" )
    private static void debug( int txNumber, String format, Object... values )
    {
        //      System.out.println( format( "(%d) %s", txNumber, format( format, values ) ) );
    }

    private static Object randomValue()
    {
        if ( RANDOM.nextBoolean() )
        {
            return primitives.get( RANDOM.nextInt( primitives.size() ) ).random();
        }
        else
        {
            return arrays.get( RANDOM.nextInt( arrays.size() ) ).random();
        }
    }

    private static String randomString()
    {
        StringBuilder stringBuilder = new StringBuilder();
        int length = RANDOM.nextInt( MAX_STRING_LENGTH );

        while ( stringBuilder.length() < length )
        {
            int c = RANDOM.nextInt( 0x10FFFF ); // highest unicode codepoint
            if ( Character.isLetterOrDigit( c ) || Character.isWhitespace( c ) )
            {
                stringBuilder.append( Character.toChars( c ) );
            }
        }

        return stringBuilder.toString();
    }

    private static DateValue randomDate()
    {
        return DateValue.ordinalDate( RANDOM.nextInt( 5000 ) - 2000, 1 + RANDOM.nextInt( 364 ) );
    }

    private static LocalTimeValue randomLocalTime()
    {
        return LocalTimeValue.localTime( RANDOM.nextInt( 24 ), RANDOM.nextInt( 60 ), RANDOM.nextInt( 60 ), RANDOM.nextInt( 1000000000 ) );
    }

    private static ZoneId randomZoneId()
    {
        return ZoneId.of( AVAILABLE_TIMEZONES[RANDOM.nextInt( AVAILABLE_TIMEZONES.length )] );
    }

    private static DateTimeValue randomDateTime()
    {
        return DateTimeValue.datetime( randomDate(), randomLocalTime(), randomZoneId() );
    }

    private static LocalDateTimeValue randomLocalDateTime()
    {
        return LocalDateTimeValue.localDateTime( randomDate(), randomLocalTime() );
    }

    private static TimeValue randomTime()
    {
        return TimeValue.select( randomLocalTime(), GraphOperations::randomZoneId );
    }

    private static DurationValue randomDuration()
    {
        if ( RANDOM.nextBoolean() )
        {
            // Based on Java period (years, months and days)
            return DurationValue.duration( Period.of( RANDOM.nextInt( 10 ), RANDOM.nextInt( 12 ), RANDOM.nextInt( 30 ) ) );
        }
        else
        {
            // Based on java duration (seconds)
            return DurationValue.duration( Duration.of( RANDOM.nextInt( 31536000 ), SECONDS ) );
        }
    }

    /** Need to treat points differently because the entire array must have the same CRS */
    private static Object arrayOfPoint( CoordinateReferenceSystem crs )
    {
        int length = 1 + RANDOM.nextInt( 5 );
        PointValue[] points = new PointValue[length];
        for ( int i = 0; i < length; i++ )
        {
            points[i] = randomPoint( crs );
        }
        return points;
    }

    private static PointValue randomPoint( CoordinateReferenceSystem crs )
    {
        double[] coord = new double[crs.getDimension()];
        for ( int i = 0; i < crs.getDimension(); i++ )
        {
            coord[i] = RANDOM.nextDouble();
        }
        return Values.pointValue( crs, coord );
    }

    private static CoordinateReferenceSystem randomCRS()
    {
        if ( ENABLE_SPATIAL_3D )
        {
            return KnownCRS[RANDOM.nextInt( KnownCRS.length )];
        }
        else
        {
            return KnownCRS[RANDOM.nextInt( KnownCRS.length - 2 )];
        }
    }

    private static char randomSingleCharCodePoint()
    {
        // TODO: This is here, because lucene does not correctly index invalid characters. The proper fix would be
        // to amend our indexing solution to handle characters differently from strings (store them as ints, for instance)
        // which would solve the problem. However, that requires a store format change which will break all indexed
        // chars that exist today. We can't do that right now, because we've release the 2.0 RC, and breaking the
        // store format for GA for a bug that is *incredibly* unlikely to happen in the wild feels like it's not
        // worth it. So, we fix this properly after GA, and until then, we give the database valid chars to index.
        do
        {
            int c = RANDOM.nextInt( 0xFFFF );
            if ( Character.charCount( c ) == 1 && (Character.isLetterOrDigit( c ) || Character.isWhitespace( c )) )
            {
                return Character.toChars( c )[0];
            }
        }
        while ( true );
    }

    static String randomKey()
    {
        return "p" + RANDOM.nextInt( PROPERTIES_COUNT );
    }

    static String randomKeyButNot( String key )
    {
        String suggestion = randomKey();
        return suggestion.equals( key ) ? randomKeyButNot( key ) : suggestion;
    }

    static Label randomLabel()
    {
        return label( "Label-" + RANDOM.nextInt( LABELS_COUNT ) );
    }

    static RelationshipType randomRelType()
    {
        return RelType.values()[RANDOM.nextInt( RelType.values().length )];
    }

    static IndexDefinition chooseRandomIndex( Iterable<IndexDefinition> indexes )
    {
        return chooseRandom( filter( NOT_CONSTRAINT, indexes ) );
    }

    static <T> T chooseRandom( final Iterable<T> iterable )
    {
        ArrayList<T> items = new ArrayList<>()
        {{
            for ( T item : iterable )
            {
                add( item );
            }
        }};
        if ( items.isEmpty() )
        {
            return null;
        }
        return items.get( RANDOM.nextInt( items.size() ) );
    }

    public void doBatchOfOperations( int count, Operation... chooseBetweenOpsOrNoneForRandom )
    {
        int txNumber = GraphOperations.tx.incrementAndGet();
        Transaction tx = db.beginTx();

        try
        {
            Node refNode = referenceNodeStrategy.find( db, tx );

            if ( refNode.getId() != 0 )
            {
                debug( txNumber, "Created reference node %s", refNode );
            }

            readLock( tx, refNode );
            for ( int i = 0; i < count; i++ )
            {
                Node nodex = findRandomNode( db, refNode, tx, txNumber );
                final Node node = nodex != null ? nodex : refNode;
                Operation operation = selectRandomOperation( chooseBetweenOpsOrNoneForRandom );
                if ( operation != null )
                {
                    operation.perform( db, txNumber, tx, node );
                }
            }
            tx.commit();
        }
        catch ( OperationRollbackException e )
        {
            debug( txNumber, "Exception: %s", e );
            tx.rollback();
        }
        finally
        {
            try
            {
                tx.close();
            }
            catch ( ConstraintViolationException ignore )
            {
                // happens for node/relationship property existence constraints
            }
        }
    }

    private void readLock( Transaction tx, Node node )
    {
        if ( acquireReadLocks )
        {
            tx.acquireReadLock( node );
        }
    }

    private void readLock( Transaction tx, Relationship relationship )
    {
        if ( acquireReadLocks )
        {
            tx.acquireReadLock( relationship );
        }
    }

    public void doSchemaOperation( SchemaOperation[] operationOrNoneForRandom )
    {
        try ( Transaction tx = db.beginTx() )
        {
            selectRandomOperation( operationOrNoneForRandom ).execute( db, tx );
            tx.commit();
        }
    }

    private SchemaOperation selectRandomOperation( SchemaOperation[] operationOrNoneForRandom )
    {
        operationOrNoneForRandom = operationOrNoneForRandom.length > 0 ? operationOrNoneForRandom : SchemaOperations.values();
        return randomOperation( operationOrNoneForRandom );
    }

    private Operation randomOperation()
    {
        while ( true )
        {
            Operation op = Operation.values()[RANDOM.nextInt( Operation.values().length )];
            if ( !op.isDebug() )
            {
                return op;
            }
        }
    }

    private Operation selectRandomOperation( Operation... chooseBetweenOpsOrNoneForRandom )
    {
        Operation operation = null;
        while ( operation == null )
        {
            Operation op = chooseBetweenOpsOrNoneForRandom.length > 0 ? randomOperation( chooseBetweenOpsOrNoneForRandom ) : randomOperation();
            if ( op.isDelete() )
            {
                // There's only 10% chance (of the initial chance) that the selected
                // operation is a delete operation of some kind. For the other 90% just
                // select a new randomly. This is to be certain that the db grows all the time.
                if ( RANDOM.nextFloat() > 0.1 )
                {
                    continue;
                }
            }
            operation = op;
        }
        return operation;
    }

    private <O> O randomOperation( O[] possibleOperations )
    {
        return possibleOperations[RANDOM.nextInt( possibleOperations.length )];
    }

    private Node findRandomNode( GraphDatabaseService databaseService, Node startNode, Transaction tx, int txNumber )
    {
        int randomMaxDepth = RANDOM.nextInt( maxDepth - 2 ) + 2;  // maxDepth - RANDOM.nextInt( RANDOM.nextInt(
        // maxDepth ) );
        Node node = startNode;
        int atLevel = 0;
        Relationship rel = null;
        for ( int i = 0; i < randomMaxDepth; i++, atLevel++ )
        {
            List<Relationship> rels = new ArrayList<>();
            for ( Relationship relx : node.getRelationships( Direction.OUTGOING, RelType.TREE ) )
            {
                rels.add( relx );
            }
            if ( rels.isEmpty() )
            {
                break;
            }
            rel = rels.get( RANDOM.nextInt( rels.size() ) );

            // Re-get and lock in case it was deleted
            rel = tx.getRelationshipById( rel.getId() );
            readLock( tx, rel );
            node = rel.getEndNode();
            readLock( tx, node );
        }

        debug( txNumber, "Found %s through %s", node, rel );
        return node;
    }

    private enum RelType implements RelationshipType
    {
        TREE,
        EXTRA
    }

    private enum TYPES
    {
        BOOLEAN
                {
                    @Override
                    protected Object random()
                    {
                        return RANDOM.nextBoolean();
                    }
                },
        BYTE
                {
                    @Override
                    protected Object random()
                    {
                        return (byte) RANDOM.nextInt( 1 << 8 );
                    }
                },
        CHAR
                {
                    @Override
                    protected Object random()
                    {
                        return randomSingleCharCodePoint();
                    }
                },
        DOUBLE
                {
                    @Override
                    protected Object random()
                    {
                        return RANDOM.nextDouble();
                    }
                },
        FLOAT
                {
                    @Override
                    protected Object random()
                    {
                        return RANDOM.nextFloat();
                    }
                },
        INT
                {
                    @Override
                    protected Object random()
                    {
                        return RANDOM.nextInt();
                    }
                },
        LONG
                {
                    @Override
                    protected Object random()
                    {
                        return RANDOM.nextLong();
                    }
                },
        SHORT
                {
                    @Override
                    protected Object random()
                    {
                        return (short) RANDOM.nextInt();
                    }
                },
        STRING
                {
                    @Override
                    protected Object random()
                    {
                        return randomString();
                    }
                },
        POINT( ENABLE_SPATIAL, ENABLE_NEW_ARRAYS )
                {
                    @Override
                    protected Object random()
                    {
                        return randomPoint( randomCRS() );
                    }

                    @Override
                    protected Object array()
                    {
                        return arrayOfPoint( randomCRS() );
                    }
                },
        DATE( ENABLE_TEMPORAL, ENABLE_NEW_ARRAYS )
                {
                    @Override
                    protected Object random()
                    {
                        return randomDate().asObject();
                    }
                },
        LOCALDATETIME( ENABLE_TEMPORAL, ENABLE_NEW_ARRAYS )
                {
                    @Override
                    protected Object random()
                    {
                        return randomLocalDateTime().asObject();
                    }
                },
        DATETIME( ENABLE_TEMPORAL, ENABLE_NEW_ARRAYS )
                {
                    @Override
                    protected Object random()
                    {
                        return randomDateTime().asObject();
                    }
                },
        LOCALTIME( ENABLE_TEMPORAL, ENABLE_NEW_ARRAYS )
                {
                    @Override
                    protected Object random()
                    {
                        return randomLocalTime().asObject();
                    }
                },
        TIME( ENABLE_TEMPORAL, ENABLE_NEW_ARRAYS )
                {
                    @Override
                    protected Object random()
                    {
                        return randomTime().asObject();
                    }
                },
        DURATION( ENABLE_TEMPORAL, ENABLE_NEW_ARRAYS )
                {
                    @Override
                    protected Object random()
                    {
                        return randomDuration().asObject();
                    }
                };
        protected boolean enable;
        protected boolean enableArrays;

        TYPES()
        {
            this( true, true );
        }

        TYPES( boolean enable, boolean enableArrays )
        {
            this.enable = enable;
            this.enableArrays = enableArrays;
        }

        protected abstract Object random();

        protected Object array()
        {
            int length = 1 + RANDOM.nextInt( 5 );
            Object ref = random();
            Object o = Array.newInstance( ref.getClass(), length );
            for ( int i = 0; i < length; i++ )
            {
                Array.set( o, i, random() );
            }
            return o;
        }
    }

    public enum Operation
    {
        createNode
                {
                    @Override
                    void perform( GraphDatabaseService databaseService, int txNumber, Transaction tx, Node node )
                    {
                        Node child = null;
                        Relationship rel = null;
                        try
                        {
                            tx.acquireWriteLock( node );
                            child = tx.createNode();
                            rel = node.createRelationshipTo( child, RelType.TREE );
                        }
                        catch ( Throwable t )
                        {
                            StringWriter writer = new StringWriter();
                            PrintWriter printWriter = new PrintWriter( writer );
                            t.printStackTrace( printWriter );
                            printWriter.close();
                            debug( txNumber, "EXCEPTION WHILE CREATING NODE: %s, %s", child, writer.toString() );
                            throw t;
                        }
                        finally
                        {
                            debug( txNumber, "Created %s with %s to %s", child, rel, node );
                        }
                    }
                },
        assertIsLeader( false, true )
                {
                    @Override
                    void perform( GraphDatabaseService databaseService, int txNumber, Transaction tx, Node node )
                    {
                        if ( Orchestrator.isLeader( (GraphDatabaseAPI) databaseService ) )
                        {
                            return;
                        }
                        throw new RuntimeException( "Is not master." );
                    }
                },
        createRollingBackTx( false, true )
                {
                    @Override
                    void perform( GraphDatabaseService databaseService, int txNumber, Transaction tx, Node node )
                    {
                        Node newNode = tx.createNode();
                        debug( txNumber, "Rolling back creation of %s", newNode );
                        throw new OperationRollbackException();
                    }
                },
        createRelationship
                {
                    @Override
                    void perform( GraphDatabaseService databaseService, int txNumber, Transaction tx, Node node )
                    {
                        Relationship aRel = findRandomRelationship( node );
                        if ( aRel == null )
                        {
                            return;
                        }
                        Node otherNode = aRel.getOtherNode( node );
                        for ( Relationship rel : node.getRelationships( RelType.EXTRA ) )
                        {
                            otherNode = rel.getOtherNode( node );
                            if ( Math.random() > 0.7 )
                            {
                                break;
                            }
                        }
                        Relationship rel = node.createRelationshipTo( otherNode, RelType.EXTRA );
                        debug( txNumber, "Created %s from %s to %s", rel, node, otherNode );
                    }
                },
        setNodeProperty
                {
                    @Override
                    void perform( GraphDatabaseService databaseService, int txNumber, Transaction tx, Node node )
                    {
                        tx.acquireWriteLock( node );

                        String key = randomKey();
                        Object value = randomValue();
                        try
                        {
                            node.setProperty( key, value );
                        }
                        catch ( ConstraintViolationException e )
                        {
                            // We expect to get this occasionally.
                            return;
                        }
                        debug( txNumber, "Set property %s = %s on %s", key, value, node );
                    }
                },
        removeNodeProperty
                {
                    @Override
                    void perform( GraphDatabaseService databaseService, int txNumber, Transaction tx, Node node )
                    {
                        tx.acquireWriteLock( node );

                        String key = randomKey();
                        node.removeProperty( key );
                        debug( txNumber, "Removed property %s from %s", key, node );
                    }
                },
        setRelationshipProperty
                {
                    @Override
                    void perform( GraphDatabaseService databaseService, int txNumber, Transaction tx, Node node )
                    {
                        Relationship rel = findRandomRelationship( node );
                        if ( rel != null )
                        {
                            tx.acquireWriteLock( rel );

                            String key = randomKey();
                            Object value = randomValue();
                            rel.setProperty( key, value );
                            debug( txNumber, "Set property %s = %s on %s", key, value, rel );
                        }
                    }
                },
        removeRelationshipProperty
                {
                    @Override
                    void perform( GraphDatabaseService databaseService, int txNumber, Transaction tx, Node node )
                    {
                        Relationship rel = findRandomRelationship( node );
                        if ( rel != null )
                        {
                            tx.acquireWriteLock( rel );

                            String key = randomKey();
                            rel.removeProperty( key );
                            debug( txNumber, "Removed property %s from %s", key, rel );
                        }
                    }
                },
        deleteRelationship
                {
                    @Override
                    void perform( GraphDatabaseService databaseService, int txNumber, Transaction tx, Node node )
                    {
                        Relationship rel = findRandomRelationship( node );
                        if ( rel != null )
                        {
                            tx.acquireWriteLock( rel );

                            debug( txNumber, "Deleted %s from %s to %s", rel, rel.getStartNode(), rel.getEndNode() );
                            deleteRelationship( rel );
                        }
                    }
                },
        deleteNode( true, false )
                {
                    @Override
                    void perform( GraphDatabaseService databaseService, int txNumber, Transaction tx, Node node )
                    {
                        Node leafNode = findRandomLeafNode( tx, node );
                        if ( !isAReferenceNode( leafNode ) )
                        {
                            tx.acquireWriteLock( leafNode );

                            List<Relationship> deletedRelationships = new ArrayList<>();
                            for ( Relationship rel : leafNode.getRelationships() )
                            {
                                deleteRelationship( rel );
                                deletedRelationships.add( rel );
                            }
                            leafNode.delete();
                            debug( txNumber, "Deleted %s along with %s", leafNode, deletedRelationships );
                        }
                    }
                },
        traverse
                {
                    @Override
                    void perform( GraphDatabaseService databaseService, int txNumber, Transaction tx, Node node )
                    {
                        // Just traverse the whole tree
                        count( tx.traversalDescription().uniqueness( RELATIONSHIP_PATH ).depthFirst().traverse( node ) );
                    }
                },
        addNodeLabel
                {
                    @Override
                    void perform( GraphDatabaseService databaseService, int txNumber, Transaction tx, Node node )
                    {
                        try
                        {
                            tx.acquireWriteLock( node );
                            Label label = randomLabel();
                            node.addLabel( label );
                            debug( txNumber, "Added %s to %s", label, node );
                        }
                        catch ( ConstraintViolationException e )
                        {
                            // We expect to get this occasionally.
                        }
                    }
                },
        removeNodeLabel
                {
                    @Override
                    void perform( GraphDatabaseService databaseService, int txNumber, Transaction tx, Node node )
                    {
                        tx.acquireWriteLock( node );
                        Label label = chooseRandom( node.getLabels() );
                        if ( label == null || REFERENCE_NODE_LABEL.name().equals( label.name() ) )
                        {
                            return;
                        }
                        node.removeLabel( label );
                        debug( txNumber, "Removed %s from %s", label, node );
                    }
                };

        private final boolean isDelete;
        private final boolean isDebug;

        Operation()
        {
            this( false, false );
        }

        Operation( boolean isDelete, boolean isDebug )
        {
            this.isDelete = isDelete;
            this.isDebug = isDebug;
        }

        abstract void perform( GraphDatabaseService databaseService, int txNumber, Transaction tx, Node node );

        boolean isDelete()
        {
            return isDelete;
        }

        boolean isDebug()
        {
            return isDebug;
        }
    }

    interface TypeWrapper
    {
        Object random();
    }
}
