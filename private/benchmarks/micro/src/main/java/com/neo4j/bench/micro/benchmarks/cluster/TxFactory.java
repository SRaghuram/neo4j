/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.micro.benchmarks.cluster;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TreeSet;
import java.util.stream.IntStream;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.io.ByteUnit;

import static java.lang.StrictMath.min;

public class TxFactory
{
    public static void commitTx( int txSize, GraphDatabaseService db )
    {
        PathBuilder pathBuilder = createBuilder( txSize );
        try ( Transaction tx = db.beginTx() )
        {
            pathBuilder.build( tx );
            tx.commit();
        }
    }

    public static void commitOneNodeTx( int txSize, GraphDatabaseService db )
    {
        PathBuilder pathBuilder = createOneNodeBuilder( txSize );
        try ( Transaction tx = db.beginTx() )
        {
            pathBuilder.build( tx );
            tx.commit();
        }
    }

    /**
     * NOTE! Model is based on the current property and label sizes. If anything changes the size of the property set or labels the model needs changing.
     * <p>
     * So here goes: t = N ( N0 + L + P ) + R ( R0 + P) + C
     * Where:
     * > t = tx size
     * > N = number of nodes
     * > N0 empty node size
     * > L = labels size
     * > P = properties size
     * > R = number of relationships
     * > R0 = empty relationship
     * > C = empty tx size
     * <p>
     * In a path R =  N -1
     * <p>
     * This gives: N = ( t - C + R0 + P ) / ( N0 + R0  + L  + 2P )
     * <p>
     * We set L and P depending on the tx size.
     * Set some constant values for C, R0, N0 from experiments and then calculate how many nodes should be used to get the desired size.
     * <p>
     * The model is a bit shaky for < 100KB tx, but still within ~ 50% diff from desired size.
     * Above 1MB the difference seems to be kept at a few %. Its OK for what is intended for in the benchmarks.
     *
     * @param txSize desired size of the serialized tx. Will be approximate.
     * @return A path builder that when build creates a path of nodes and relationships to represent provided size as close as possible
     */
    private static PathBuilder createBuilder( int txSize )
    {
        int[] sizes = splitInto( txSize, Collections.singletonList( ByteUnit.bytes( PathBuilder.PROPERTY_SET_SIZE ) ) );

        int nrOfPropSets = sizes[0];
        int extraBytes = sizes[1];

        int labelSize = 15;

        nrOfPropSets = min( 20, nrOfPropSets );

        int labels = min( 10, extraBytes / labelSize );

        int p = nrOfPropSets * PathBuilder.PROPERTY_SET_SIZE;
        int l = labels * labelSize;
        int nodesInPath = (int) Math.round( (txSize + p) / (double) (2 * p + l + 170) );

        return new PathBuilder()
                .nodesInPath( nodesInPath )
                .propertySetsPerRelationship( nrOfPropSets )
                .propertySetsPerNode( nrOfPropSets )
                .labelsPerNode( labels );
    }

    private static PathBuilder createOneNodeBuilder( int txSize )
    {
        int labelSize = 15;
        return new PathBuilder()
                .nodesInPath( 1 )
                .propertySetsPerRelationship( 0 )
                .propertySetsPerNode( 0 )
                .bulkPropertySize( txSize - labelSize )
                .labelsPerNode( 1 );
    }

    private static int[] splitInto( long totalSize, Collection<Long> sizes )
    {
        TreeSet<Long> reversedOrderSet = new TreeSet<>( ( o1, o2 ) -> -1 * Long.compare( o1, o2 ) );
        reversedOrderSet.addAll( sizes );

        int[] split = new int[reversedOrderSet.size() + 1];
        int index = 0;

        long bytesLeft = totalSize;

        for ( Long size : reversedOrderSet )
        {
            long timesSize = bytesLeft / size;
            split[index] = (int) timesSize;
            index++;
            bytesLeft -= timesSize * size;
        }

        split[index] = (int) bytesLeft;
        return split;
    }

    private static class PathBuilder
    {
        private int propertySetsPerNode = 1;
        private int labelsPerNode = 1;
        private int propertiesPerRelationship = 1;
        private int nodesInPath = 2;
        private int bulkPropertySize;

        private Random rnd = new Random();

        private static final int PROPERTY_SET_SIZE = 5874;
        private static final List<Object> PROPERTY_SET = new ArrayList<>()
        {{
            add( "Kristersson föreslås som statsminister\n" +
                 "I nästa vecka kommer riksdagen få rösta om Moderatledaren Ulf Kristersson som statsminister. " +
                 "Talmannen Andreas Norlén ger honom fram till dess att " +
                 "undersöka vilka partier som ska ingå i hans regering.\n" +
                 "\n" + "\n" + "TT\n" + "\n" + "Talman Andreas Norl n anländer till riksdagens pressrum.\n" + "Bild: Pontus Lundahl/TT\n" +
                 "Det finns inga förutsättningar att utse en ny sonderingsperson, på det sätt som Ulf Kristersson och S-ledaren Stefan " +
                 "Löfven tidigare har haft i " +
                 "uppdrag att sondera läget. Möjligheterna är uttömda, anser talmannen.\n" +
                 "\n" + "I stället tänker Andreas Norlén nästa måndag föreslå att riksdagen ska få rösta om Kristersson som statsminister.\n" +
                 "Talmannen är dock inte säker på att Kristersson kommer att bli vald.\n" +
                 "– För dagen finns inga som helst garantier för att Ulf Kristersson blir vald, säger Andreas Norlén.\n" +
                 "Ulf Kristersson har tidigare föreslagit olika regeringsalternativ där ett eller flera Allianspartier ingår, alla med " +
                 "honom själv som statsminister." +
                 " De har dock stoppats av att både Centern och Liberalerna sagt nej, eftersom dessa alternativ behöver stöd av " +
                 "Sverigedemokraterna för att få igenom sin politik. Även SD säger nej till Kristerssons alternativ, så länge de " +
                 "inte får inflytande över politiken.\n" +
                 "När riksdagen nu får rösta om Kristersson tvingas C och L att välja om de ska stå fast vid sina nej eller släppa " +
                 "fram Moderatledaren som statsminister.\n" +
                 "Norlén räknar med att riksdagen skulle kunna rösta om Kristersson som statsminister den 14 november.\n" +
                 "– Men det är inte ett definitivt besked, säger Norlén.\n" +
                 "Den 14 november är dagen innan riksdagen ska debattera budgeten. Därför kan omröstningen komma att ske senare. Norlén understryker dock " +
                 "att det är viktigt att ta ett nytt steg i regeringssamtalen.\n" +
                 "– Processen måste föras framåt, dynamiken mellan partierna måste förändras, de resultatlösa samtalen måste få ett slut, säger Norlén.\n" +
                 "Hittills har sonderingarna ledda av M-ledaren Ulf Kristersson respektive S-ledaren Stefan Löfven inte gett några resultat, inte " +
                 "heller talmannens gruppsamtal med företrädare för fyra tänkbara regeringsalternativ.\n" +
                 "En möjlighet som har förts fram är att C-ledaren Annie Lööf skulle få ett nytt sonderingsuppdrag.\n" +
                 "– Annie Lööf har sagt att hon är redo att åta sig ett sonderingsuppdrag för att undersöka en regeringssamverkan mellan " +
                 "Allianspartierna och Miljöpartiet om hela Alliansen står bakom ett sådant uppdrag, säger Andreas Norlén.\n" +
                 "– Det sista föreligger dock inte eftersom två av fyra Allianspartier inte anser att ett sådant uppdrag ska ges.\n" );

            add( Integer.MAX_VALUE );
            add( new int[]{1, 2, 3, 4, 5, 6, 7, 8, 9, 10} );
            add( "Kristersson föreslås som statsminister" );
            add( new String[]{
                    "På söndagskvällen hördes en hög smäll över stora delar av Malmö. Någon form av explosion hade inträffat vid en trafikskola på" +
                    " Norra Grängesbergsgatan och polisen spärrade av ett större område i väntan på bombtekniker.",
                    "\n" + "AvJulius ViktorssonOlov PlanthaberPatrick Persson\n" + "\n" + "Bild: Patrick Persson\n",
                    "Vid 22.30-tiden hördes en hög smäll över stora delar av Malmö. Läsare som hörde av sig till tidningen berättade" +
                    " att smällen hördes hela vägen från Limhamn till Arlöv.\n",
                    "Enligt polisen hade det skett någon form av explosion vid en fastighet på Norra Grängesbergsgatan. " +
                    "Flera fönsterrutor hade krossats. Polisen spärrade av ett större område längs Uddeholmsgatan, Västanforsgatan," +
                    " Norra Grängesbergsgatan ner mot Amiralsgatan samt Kopparbergsgatan.\n",
                    "\n" + "\n" + "Bild: Patrick Persson\n",
                    "Ingen person befann sig i fastigheten när explosionen skedde och ingen person hittades skadad." +
                    " Polisens bombtekniker beställdes till platsen.\n",
                    "– Det kommer göras sedvanliga utredningsåtgärder och göras teknisk undersökning, sa Erik Liljenström," +
                    " inre befäl vid Malmöpolisen, kort efter händelsen.\n",
                    "Läs mer: Bomben på Persborg: Grannar hörde en kraftig smäll och såg ett starkt rött sken\n",
                    "Explosionen är den andra i Malmö på kort tid: Strax efter midnatt natten till lördagen exploderade någon" +
                    " form av sprängladdning vid en trafikskola på Persborgstorget. Ingen person skadades vid det tillfället och" +
                    " enligt polisen fanns det ingen känd hotbild mot trafikskolan.\n",
                    "Ser ni någon koppling mellan den här händelsen och sprängningen på Persborgstorget?\n",
                    "– Det kan jag inte uttala mig om, säger Erik Liljenström.\n" + "Polisen utreder händelsen som allmänfarlig ödeläggelse.\n",
                    "Trafikskolan ligger tillsammans med andra verksamheter på bottenvåningen i en trevånings kontorsbyggnad." +
                    " Fönsterrutor på bottenvåning och andra våningen krossades vid sprängningen.\n",
                    "Vid 08.30-tiden rapporterade TT att avspärrningarna kring fastigheten var hävda."} );
        }};

        PathBuilder propertySetsPerNode( int propertiesPerNode )
        {
            this.propertySetsPerNode = propertiesPerNode;
            return this;
        }

        PathBuilder labelsPerNode( int labelsPerNode )
        {
            this.labelsPerNode = labelsPerNode;
            return this;
        }

        PathBuilder propertySetsPerRelationship( int propertiesPerRelationship )
        {
            this.propertiesPerRelationship = propertiesPerRelationship;
            return this;
        }

        PathBuilder bulkPropertySize( int bulkPropertySize )
        {
            this.bulkPropertySize = bulkPropertySize;
            return this;
        }

        PathBuilder nodesInPath( int nodesInPath )
        {
            this.nodesInPath = nodesInPath;
            return this;
        }

        void build( Transaction tx )
        {
            Map<String,Object> nodeProperties = createNodeProperties();
            Map<String,Object> relationshipProperties = createRelationshipProperties();
            Label[] nodeLabels = createNodeLabels();

            Node prevNode = createNode( tx, nodeProperties, nodeLabels );
            for ( int i = 1; i < nodesInPath; i++ )
            {
                Node currentNode = createNode( tx, nodeProperties, nodeLabels );
                Relationship link = prevNode.createRelationshipTo( currentNode, RelationshipType.withName( "THIS_IS_A_LABEL_TYPE" ) );
                fillRelationship( link, relationshipProperties );
                prevNode = currentNode;
            }
        }

        private Label[] createNodeLabels()
        {
            return IntStream.range( 0, labelsPerNode ).mapToObj( i -> Label.label( "THIS_LABEL_HAS_NUMBER_" + i ) ).toArray( Label[]::new );
        }

        private void fillRelationship( Relationship link, Map<String,Object> relationshipProperties )
        {
            relationshipProperties.forEach( link::setProperty );
        }

        private Node createNode( Transaction tx, Map<String,Object> nodeProperties, Label[] nodeLabels )
        {
            Node node = tx.createNode( nodeLabels );
            nodeProperties.forEach( node::setProperty );
            return node;
        }

        private Map<String,Object> createRelationshipProperties()
        {
            return createProperties( propertiesPerRelationship, 0 );
        }

        private Map<String,Object> createNodeProperties()
        {
            return createProperties( propertySetsPerNode, bulkPropertySize );
        }

        private Map<String,Object> createProperties( int propertySets, int bulkPropertySize )
        {
            HashMap<String,Object> map = new HashMap<>();
            for ( int i = 0; i < propertySets; i++ )
            {
                for ( int j = 0; j < PROPERTY_SET.size(); j++ )
                {
                    map.put( "THIS_IS_KEY_" + i + "_IN_SET_" + j, PROPERTY_SET.get( j ) );
                }
            }
            if ( bulkPropertySize > 0 )
            {
                var bytes = new byte[ bulkPropertySize ];
                rnd.nextBytes( bytes );
                map.put( "THIS_IS_KEY_FOR_BULK", bytes );
            }
            return map;
        }
    }
}
