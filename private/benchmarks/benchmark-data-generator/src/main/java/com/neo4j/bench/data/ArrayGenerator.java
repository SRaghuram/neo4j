/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.data;

import com.fasterxml.jackson.annotation.JsonTypeInfo;

import java.util.Objects;
import java.util.SplittableRandom;

import org.neo4j.values.storable.ArrayValue;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

public class ArrayGenerator
{
    public static final int DEFAULT_SIZE = 50;

    public static ArrayGeneratorFactory<String[],String> stringArray(
            ValueGeneratorFactory<String> elementGeneratorFactory,
            int arrayLength )
    {
        return new ArrayGeneratorFactory<>( new StringArrayGeneratorFun(), elementGeneratorFactory, arrayLength );
    }

    public static ArrayGeneratorFactory<short[],Short> shortArray(
            ValueGeneratorFactory<Short> elementGeneratorFactory,
            int arrayLength )
    {
        return new ArrayGeneratorFactory<>( new ShortArrayGeneratorFun(), elementGeneratorFactory, arrayLength );
    }

    public static ArrayGeneratorFactory<byte[],Byte> byteArray(
            ValueGeneratorFactory<Byte> elementGeneratorFactory,
            int arrayLength )
    {
        return new ArrayGeneratorFactory<>( new ByteArrayGeneratorFun(), elementGeneratorFactory, arrayLength );
    }

    public static ArrayGeneratorFactory<int[],Integer> intArray(
            ValueGeneratorFactory<Integer> elementGeneratorFactory,
            int arrayLength )
    {
        return new ArrayGeneratorFactory<>( new IntArrayGeneratorFun(), elementGeneratorFactory, arrayLength );
    }

    public static ArrayGeneratorFactory<long[],Long> longArray(
            ValueGeneratorFactory<Long> elementGeneratorFactory,
            int arrayLength )
    {
        return new ArrayGeneratorFactory<>( new LongArrayGeneratorFun(), elementGeneratorFactory, arrayLength );
    }

    public static ArrayGeneratorFactory<float[],Float> floatArray(
            ValueGeneratorFactory<Float> elementGeneratorFactory,
            int arrayLength )
    {
        return new ArrayGeneratorFactory<>( new FloatArrayGeneratorFun(), elementGeneratorFactory, arrayLength );
    }

    public static ArrayGeneratorFactory<double[],Double> doubleArray(
            ValueGeneratorFactory<Double> elementGeneratorFactory,
            int arrayLength )
    {
        return new ArrayGeneratorFactory<>( new DoubleArrayGeneratorFun(), elementGeneratorFactory, arrayLength );
    }

    public static class ArrayGeneratorFactory<GENERATOR_TYPE, ELEMENT_TYPE> implements ValueGeneratorFactory<GENERATOR_TYPE>
    {
        private ArrayGeneratorFun<GENERATOR_TYPE,ELEMENT_TYPE> arrayGeneratorFun;
        private ValueGeneratorFactory<ELEMENT_TYPE> elementGeneratorFactory;
        private int arrayLength;

        private ArrayGeneratorFactory()
        {
        }

        private ArrayGeneratorFactory(
                ArrayGeneratorFun<GENERATOR_TYPE,ELEMENT_TYPE> arrayGeneratorFun,
                ValueGeneratorFactory<ELEMENT_TYPE> elementGeneratorFactory,
                int arrayLength )
        {
            this.arrayGeneratorFun = arrayGeneratorFun;
            this.elementGeneratorFactory = elementGeneratorFactory;
            this.arrayLength = arrayLength;
        }

        @Override
        public ValueGeneratorFun<GENERATOR_TYPE> create()
        {
            // TODO unless generator is singleton every array will have same elements when using deterministic gen
            // TODO create ValueGeneratorFactory that creates ascending generators, where 'initial' slides upwards on
            // TODO each new generator
            final ValueGeneratorFun<ELEMENT_TYPE> valueGenerator = elementGeneratorFactory.create();
            return new ValueGeneratorFun<GENERATOR_TYPE>()
            {
                @Override
                public boolean wrapped()
                {
                    return false;
                }

                @Override
                public GENERATOR_TYPE next( SplittableRandom rng )
                {
                    return arrayGeneratorFun.generateArray( arrayLength, rng, valueGenerator );
                }

                @Override
                public Value nextValue( SplittableRandom rng )
                {
                    return arrayGeneratorFun.generateArrayValue( arrayLength, rng, valueGenerator );
                }
            };
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
            ArrayGeneratorFactory<?,?> that = (ArrayGeneratorFactory<?,?>) o;
            return arrayLength == that.arrayLength &&
                   arrayGeneratorFun.getClass().equals( that.arrayGeneratorFun.getClass() ) &&
                   Objects.equals( elementGeneratorFactory, that.elementGeneratorFactory );
        }

        @Override
        public int hashCode()
        {
            return Objects.hash( arrayGeneratorFun, elementGeneratorFactory, arrayLength );
        }
    }

    @JsonTypeInfo( use = JsonTypeInfo.Id.CLASS )
    interface ArrayGeneratorFun<GENERATOR_TYPE, ELEMENT_TYPE>
    {
        GENERATOR_TYPE generateArray(
                int arrayLength,
                SplittableRandom rng,
                ValueGeneratorFun<ELEMENT_TYPE> valueGenerator );

        ArrayValue generateArrayValue(
                int arrayLength,
                SplittableRandom rng,
                ValueGeneratorFun<ELEMENT_TYPE> valueGenerator );
    }

    private static class StringArrayGeneratorFun implements ArrayGeneratorFun<String[],String>
    {
        @Override
        public String[] generateArray( int arrayLength, SplittableRandom rng, ValueGeneratorFun<String> valueGenerator )
        {
            String[] array = new String[arrayLength];
            for ( int i = 0; i < arrayLength; i++ )
            {
                array[i] = valueGenerator.next( rng );
            }
            return array;
        }

        @Override
        public ArrayValue generateArrayValue( int arrayLength, SplittableRandom rng,
                                              ValueGeneratorFun<String> valueGenerator )
        {
            return Values.stringArray( generateArray( arrayLength, rng, valueGenerator ) );
        }
    }

    private static class ShortArrayGeneratorFun implements ArrayGeneratorFun<short[],Short>
    {
        @Override
        public short[] generateArray( int arrayLength, SplittableRandom rng, ValueGeneratorFun<Short> valueGenerator )
        {
            short[] array = new short[arrayLength];
            for ( int i = 0; i < arrayLength; i++ )
            {
                array[i] = valueGenerator.next( rng );
            }
            return array;
        }

        @Override
        public ArrayValue generateArrayValue( int arrayLength, SplittableRandom rng,
                                              ValueGeneratorFun<Short> valueGenerator )
        {
            return Values.shortArray( generateArray( arrayLength, rng, valueGenerator ) );
        }
    }

    private static class ByteArrayGeneratorFun implements ArrayGeneratorFun<byte[],Byte>
    {
        @Override
        public byte[] generateArray( int arrayLength, SplittableRandom rng, ValueGeneratorFun<Byte> valueGenerator )
        {
            byte[] array = new byte[arrayLength];
            for ( int i = 0; i < arrayLength; i++ )
            {
                array[i] = valueGenerator.next( rng );
            }
            return array;
        }

        @Override
        public ArrayValue generateArrayValue( int arrayLength, SplittableRandom rng,
                                              ValueGeneratorFun<Byte> valueGenerator )
        {
            return Values.byteArray( generateArray( arrayLength, rng, valueGenerator ) );
        }
    }

    private static class IntArrayGeneratorFun implements ArrayGeneratorFun<int[],Integer>
    {
        @Override
        public int[] generateArray( int arrayLength, SplittableRandom rng, ValueGeneratorFun<Integer> valueGenerator )
        {
            int[] array = new int[arrayLength];
            for ( int i = 0; i < arrayLength; i++ )
            {
                array[i] = valueGenerator.next( rng );
            }
            return array;
        }

        @Override
        public ArrayValue generateArrayValue( int arrayLength, SplittableRandom rng,
                                              ValueGeneratorFun<Integer> valueGenerator )
        {
            return Values.intArray( generateArray( arrayLength, rng, valueGenerator ) );
        }
    }

    private static class LongArrayGeneratorFun implements ArrayGeneratorFun<long[],Long>
    {
        @Override
        public long[] generateArray( int arrayLength, SplittableRandom rng, ValueGeneratorFun<Long> valueGenerator )
        {
            long[] array = new long[arrayLength];
            for ( int i = 0; i < arrayLength; i++ )
            {
                array[i] = valueGenerator.next( rng );
            }
            return array;
        }

        @Override
        public ArrayValue generateArrayValue( int arrayLength, SplittableRandom rng,
                                              ValueGeneratorFun<Long> valueGenerator )
        {
            return Values.longArray( generateArray( arrayLength, rng, valueGenerator ) );
        }
    }

    private static class FloatArrayGeneratorFun implements ArrayGeneratorFun<float[],Float>
    {
        @Override
        public float[] generateArray( int arrayLength, SplittableRandom rng, ValueGeneratorFun<Float> valueGenerator )
        {
            float[] array = new float[arrayLength];
            for ( int i = 0; i < arrayLength; i++ )
            {
                array[i] = valueGenerator.next( rng );
            }
            return array;
        }

        @Override
        public ArrayValue generateArrayValue( int arrayLength, SplittableRandom rng,
                                              ValueGeneratorFun<Float> valueGenerator )
        {
            return Values.floatArray( generateArray( arrayLength, rng, valueGenerator ) );
        }
    }

    private static class DoubleArrayGeneratorFun implements ArrayGeneratorFun<double[],Double>
    {
        @Override
        public double[] generateArray( int arrayLength, SplittableRandom rng, ValueGeneratorFun<Double> valueGenerator )
        {
            double[] array = new double[arrayLength];
            for ( int i = 0; i < arrayLength; i++ )
            {
                array[i] = valueGenerator.next( rng );
            }
            return array;
        }

        @Override
        public ArrayValue generateArrayValue( int arrayLength, SplittableRandom rng,
                                              ValueGeneratorFun<Double> valueGenerator )
        {
            return Values.doubleArray( generateArray( arrayLength, rng, valueGenerator ) );
        }
    }
}
