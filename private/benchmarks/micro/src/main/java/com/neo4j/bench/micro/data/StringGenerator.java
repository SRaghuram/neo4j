/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.micro.data;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Random;
import java.util.SplittableRandom;
import java.util.function.IntFunction;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

import static java.lang.String.format;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toList;

/*
    All inline-able short strings
    http://neo4j.com/docs/operations-manual/current/#property-compression
    org.neo4j.kernel.impl.store.LongerShortString

    String class                          Character count limit
    Numerical, Date and Hex                     54
    Uppercase, Lowercase and E-mail             43
    URI, Alpha-numerical and Alpha-symbolical   36
    European                                    31
    Latin1                                      27
    UTF-8                                       14

    The above inlineability limits are likely wrong. See
    https://github.com/neo4j/neo4j/blob/cda5a2e56224c56fff916d455db3db47ca71322b/enterprise/runtime/neole/src/main/java/org/neo4j/internal/store/prototype/neole/PropertyCursor.java#L66-L85
*/
public class StringGenerator
{
    public static final int BIG_STRING_LENGTH = 500;
    public static final int SMALL_STRING_LENGTH = 10;

    public static ValueGeneratorFactory<String> intString( ValueGeneratorFactory<Integer> integers, int stringLength )
    {
        return new NumberStringGeneratorFactory( integers, stringLength );
    }

    public static ValueGeneratorFactory<String> randShortNumerical()
    {
        return new RandomStringGeneratorFactory( new NumericalChars(), 54 );
    }

    public static ValueGeneratorFactory<String> randShortDate()
    {
        return new RandomStringGeneratorFactory( new DateChars(), 54 );
    }

    public static ValueGeneratorFactory<String> randShortHex()
    {
        return new RandomStringGeneratorFactory( new HexChars(), 54 );
    }

    public static ValueGeneratorFactory<String> randShortLower()
    {
        return new RandomStringGeneratorFactory( new LowerChars(), 43 );
    }

    public static ValueGeneratorFactory<String> randShortUpper()
    {
        return new RandomStringGeneratorFactory( new UpperChars(), 43 );
    }

    public static ValueGeneratorFactory<String> randShortEmail()
    {
        return new RandomStringGeneratorFactory( new EmailChars(), 43 );
    }

    public static ValueGeneratorFactory<String> randShortUri()
    {
        return new RandomStringGeneratorFactory( new UriChars(), 36 );
    }

    public static ValueGeneratorFactory<String> randShortAlphaNumerical()
    {
        return new RandomStringGeneratorFactory( new AlphaNumericalChars(), 36 );
    }

    public static ValueGeneratorFactory<String> randShortAlphaSymbolical()
    {
        return new RandomStringGeneratorFactory( new AlphaSymbolicChars(), 36 );
    }

    public static ValueGeneratorFactory<String> randInlinedAlphaNumerical()
    {
        return new RandomStringGeneratorFactory( new AlphaNumericalChars(), 11 );
    }

    public static ValueGeneratorFactory<String> randShortUtf8()
    {
        return new RandomStringGeneratorFactory( new Utf8Chars(), 14 );
    }

    public static ValueGeneratorFactory<String> randUtf8( int stringLength )
    {
        return new RandomStringGeneratorFactory( new Utf8Chars(), stringLength );
    }

    private static final int POPULATION_SIZE = 0xFFFF;

    public static class RandomStringGeneratorFactory implements ValueGeneratorFactory<String>
    {
        private Characters characters;
        private int stringLength;

        @JsonCreator
        public RandomStringGeneratorFactory( @JsonProperty( "characters" ) Characters characters,
                                             @JsonProperty( "stringLength" ) int stringLength )
        {
            this.characters = characters;
            this.stringLength = stringLength;
        }

        @Override
        public ValueGeneratorFun<String> create()
        {
            return new RandomStringGenerator( characters.characters(), stringLength );
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
            RandomStringGeneratorFactory that = (RandomStringGeneratorFactory) o;
            return stringLength == that.stringLength &&
                   characters.getClass().equals( that.characters.getClass() );
        }

        @Override
        public int hashCode()
        {
            return Objects.hash( characters, stringLength );
        }
    }

    public static class NumberStringGeneratorFactory implements ValueGeneratorFactory<String>
    {
        private ValueGeneratorFactory<Integer> numberGeneratorFactory;
        private int length;

        private NumberStringGeneratorFactory()
        {
        }

        public NumberStringGeneratorFactory( ValueGeneratorFactory<Integer> numberGeneratorFactory, int length )
        {
            this.numberGeneratorFactory = numberGeneratorFactory;
            this.length = length;
        }

        @Override
        public ValueGeneratorFun<String> create()
        {
            return new NumberStringGenerator( numberGeneratorFactory.create(), length );
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
            NumberStringGeneratorFactory that = (NumberStringGeneratorFactory) o;
            return length == that.length &&
                   Objects.equals( numberGeneratorFactory, that.numberGeneratorFactory );
        }

        @Override
        public int hashCode()
        {
            return Objects.hash( numberGeneratorFactory, length );
        }
    }

    private static class NumberStringGenerator implements ValueGeneratorFun<String>
    {
        // Integer can never exceed 10 digits
        private static final int INT_DIGITS = 10;
        private final IntFunction<String> paddingFun;
        private final ValueGeneratorFun<Integer> inner;
        private int state;

        private NumberStringGenerator( ValueGeneratorFun<Integer> inner, int length )
        {
            if ( length < INT_DIGITS )
            {
                throw new RuntimeException( format( "Length must be >= %s, maximum digits for 'int'", INT_DIGITS ) );
            }
            this.inner = inner;
            this.paddingFun = leftPaddingFunFor( length );
        }

        @Override
        public boolean wrapped()
        {
            return inner.wrapped();
        }

        @Override
        public String next( SplittableRandom rng )
        {
            state = inner.next( rng );
            return paddingFun.apply( state );
        }

        @Override
        public Value nextValue( SplittableRandom rng )
        {
            return Values.utf8Value( next( rng ) );
        }
    }

    public static IntFunction<String> leftPaddingFunFor( int length )
    {
        String stringPaddingFormat = "%0" + NumberStringGenerator.INT_DIGITS + "d";
        // Padding by prefixing a string is much faster than format()
        int prefixLength = Math.max( 0, length - NumberStringGenerator.INT_DIGITS );
        String prefix = IntStream.range( 0, prefixLength ).boxed().map( i -> "0" ).collect( Collectors.joining() );
        return value -> prefix + format( stringPaddingFormat, value );
    }

    private static class RandomStringGenerator implements ValueGeneratorFun<String>
    {
        private final List<Character> characters;
        private final int length;
        private String value;

        private RandomStringGenerator( List<Character> characters, int length )
        {
            this.characters = characters;
            this.length = length;
        }

        @Override
        public boolean wrapped()
        {
            return false;
        }

        @Override
        public String next( SplittableRandom rng )
        {
            createValueIfNecessary( rng );
            int i = rng.nextInt( POPULATION_SIZE - length );
            return value.substring( i, i + length );
        }

        @Override
        public Value nextValue( SplittableRandom rng )
        {
            return Values.utf8Value( next( rng ) );
        }

        private void createValueIfNecessary( SplittableRandom rng )
        {
            if ( null == value )
            {
                /*
                NOTE:
                  > really should use passed in random, but shuffle does not support SplittableRandom
                  > at least this way the seed is both controlled, and dependent on provided rng (not hardcoded)
                  > cost of constructing new Random should not be bad, as it only happens once
                */
                Random slowRng = new Random( rng.nextLong() );
                List<Character> population = extendByCloning( characters, 1 + POPULATION_SIZE / characters.size() );
                value = toShuffledString( slowRng, population ).substring( 0, POPULATION_SIZE );
            }
        }

        private List<Character> extendByCloning( List<Character> original, int clones )
        {
            List<Character> result = new ArrayList<>();
            IntStream.range( 0, clones ).boxed().forEach( i -> result.addAll( original ) );
            return result;
        }

        private String toShuffledString( Random rng, List<Character> characters )
        {
            Collections.shuffle( characters, rng );
            return characters.stream().map( Object::toString ).collect( Collectors.joining() );
        }
    }

    @JsonTypeInfo( use = JsonTypeInfo.Id.CLASS )
    private interface Characters
    {
        List<Character> characters();
    }

    // Numerical: 0..9, space, period, dash, plus, comma and apostrophe
    private static class NumericalChars implements Characters
    {
        @Override
        public List<Character> characters()
        {
            return mergeCollect(
                    Stream.of( ' ', '.', '-', '+', ' ', ',', '\'' ),
                    charRange( '0', '9' ) );
        }
    }

    // Date: 0..9, space, dash, colon, slash, plus and comma
    private static class DateChars implements Characters
    {
        @Override
        public List<Character> characters()
        {
            return mergeCollect(
                    Stream.of( ' ', '-', ':', '/', '+', ',' ),
                    charRange( '0', '9' ) );
        }
    }

    // Hex: 0..9, a..f, A..F
    private static class HexChars implements Characters
    {
        @Override
        public List<Character> characters()
        {
            return mergeCollect(
                    charRange( 'a', 'f' ),
                    charRange( 'A', 'F' ),
                    charRange( '0', '9' ) );
        }
    }

    // Lower case: a..z, space, underscore, period, dash, colon, slash
    private static class LowerChars implements Characters
    {
        @Override
        public List<Character> characters()
        {
            return mergeCollect(
                    charRange( 'a', 'z' ),
                    Stream.of( ' ', '_', '.', '-', ':', '/' ) );
        }
    }

    // Upper case: A..Z, space, underscore, period, dash, colon, slash
    private static class UpperChars implements Characters
    {
        @Override
        public List<Character> characters()
        {
            return mergeCollect(
                    charRange( 'A', 'Z' ),
                    Stream.of( ' ', '_', '.', '-', ':', '/' ) );
        }
    }

    // E-mail: a..z, comma, underscore, period, dash, plus, at sign (@)
    private static class EmailChars implements Characters
    {
        @Override
        public List<Character> characters()
        {
            return mergeCollect(
                    charRange( 'a', 'z' ),
                    Stream.of( ',', '_', '.', '-', '+', '@' ) );
        }
    }

    // URI: a..z, 0..9, and most punctuation available
    private static class UriChars implements Characters
    {
        @Override
        public List<Character> characters()
        {
            return mergeCollect(
                    charRange( 'a', 'z' ),
                    charRange( '0', '9' ),
                    Stream.of( ',', '_', '.', '-', '+', '@' ) );
        }
    }

    // Alpha-numerical: a..z, A..z, 0..9, space, underscore
    private static class AlphaNumericalChars implements Characters
    {
        @Override
        public List<Character> characters()
        {
            return mergeCollect(
                    charRange( 'a', 'z' ),
                    charRange( 'A', 'Z' ),
                    charRange( '0', '9' ),
                    Stream.of( ' ', '_' ) );
        }
    }

    // Alpha-symbolical: a..z, A..Z, space, period, comma, _ - : / + ' @ | ;
    private static class AlphaSymbolicChars implements Characters
    {
        @Override
        public List<Character> characters()
        {
            return mergeCollect(
                    charRange( 'a', 'z' ),
                    charRange( 'A', 'Z' ),
                    Stream.of( ' ', '.', ',', '_', '-', ':', '/', '+', '\'', '@', '|', ';' ) );
        }
    }

    // UTF-8
    private static class Utf8Chars implements Characters
    {
        @Override
        public List<Character> characters()
        {
            // From: http://jrgraphix.net/research/unicode_blocks.php
            return mergeCollect(
                    // Hebrew
                    charRange( 0x0590, 0x05FF ),
                    // Arabic
                    charRange( 0x0600, 0x06FF ),
                    // Hiragana
                    charRange( 0x3040, 0x309F ),
                    // Katakana
                    charRange( 0x30A0, 0x30FF ),
                    // Thai
                    charRange( 0x0E00, 0x0E7F ),
                    // Basic Latin
                    charRange( 0x0020, 0x007F ),
                    // Tibetan
                    charRange( 0x0F00, 0x0FFF ),
                    // Cyrillic
                    charRange( 0x0400, 0x04FF ) );
        }
    }

    @SafeVarargs
    private static List<Character> mergeCollect( Stream<Character>... streams )
    {
        return Stream.of( streams ).flatMap( identity() ).collect( toList() );
    }

    private static Stream<Character> charRange( int start, int end )
    {
        return IntStream.range( start, end + 1 ).mapToObj( i -> (char) i );
    }
}
