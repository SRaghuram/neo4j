/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.micro.data;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.SplittableRandom;
import java.util.function.IntFunction;

import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

import static java.lang.String.format;

public abstract class NumberGenerator
{
    public static ValueGeneratorFactory<Double> toDouble( ValueGeneratorFactory<? extends Number> factory )
    {
        return new NumberToDoubleGeneratorFactory<>( factory );
    }

    public static ValueGeneratorFactory<Float> toFloat( ValueGeneratorFactory<? extends Number> factory )
    {
        return new NumberToFloatGeneratorFactory<>( factory );
    }

    public static ValueGeneratorFactory<Byte> randByte( byte min, byte max )
    {
        return new RandomByteGeneratorFactory( min, max );
    }

    public static ValueGeneratorFactory<Short> randShort( short min, short max )
    {
        return new RandomShortGeneratorFactory( min, max );
    }

    public static ValueGeneratorFactory<Integer> randInt( int min, int max )
    {
        return new RandomIntegerGeneratorFactory( min, max );
    }

    public static ValueGeneratorFactory<Long> randLong( long min, long max )
    {
        return new RandomLongGeneratorFactory( min, max );
    }

    public static ValueGeneratorFactory<Float> randFloat( float min, float max )
    {
        return new RandomFloatGeneratorFactory( min, max );
    }

    public static ValueGeneratorFactory<Double> randDouble( double min, double max )
    {
        return new RandomDoubleGeneratorFactory( min, max );
    }

    public static ValueGeneratorFactory<Byte> ascByte( byte from )
    {
        return new AscByteGeneratorFactory( from );
    }

    public static ValueGeneratorFactory<Short> ascShort( short from )
    {
        return new AscShortGeneratorFactory( from );
    }

    public static ValueGeneratorFactory<Integer> ascInt( int from )
    {
        return new AscIntegerGeneratorFactory( from );
    }

    public static ValueGeneratorFactory<Long> ascLong( long from )
    {
        return new AscLongGeneratorFactory( from );
    }

    public static ValueGeneratorFactory<Float> ascFloat( float from )
    {
        return new AscFloatGeneratorFactory( from );
    }

    public static ValueGeneratorFactory<Double> ascDouble( double from )
    {
        return new AscDoubleGeneratorFactory( from );
    }

    public static ValueGeneratorFactory<Integer> stridingInt( int stride, int max, int offset, boolean sliding )
    {
        return new StridingNumberGeneratorFactory<>( stride, max, offset, sliding );
    }

    public static ValueGeneratorFactory<Long> stridingLong( long stride, long max, long offset, boolean sliding )
    {
        return new StridingNumberGeneratorFactory<>( stride, max, offset, sliding );
    }

    public static ValueGeneratorFactory<Float> stridingFloat( float stride, float max, float offset, boolean sliding )
    {
        return new StridingNumberGeneratorFactory<>( stride, max, offset, sliding );
    }

    public static ValueGeneratorFactory<Double> stridingDouble( double stride, double max, double offset,
            boolean sliding )
    {
        return new StridingNumberGeneratorFactory<>( stride, max, offset, sliding );
    }

    private static class NumberToDoubleGeneratorFactory<NUMBER extends Number> implements ValueGeneratorFactory<Double>
    {
        private ValueGeneratorFactory<NUMBER> valueGeneratorFactory;

        private NumberToDoubleGeneratorFactory()
        {
        }

        private NumberToDoubleGeneratorFactory( ValueGeneratorFactory<NUMBER> valueGeneratorFactory )
        {
            this.valueGeneratorFactory = valueGeneratorFactory;
        }

        @Override
        public ValueGeneratorFun<Double> create()
        {
            ValueGeneratorFun<NUMBER> valueGeneratorFun = valueGeneratorFactory.create();
            return new ValueGeneratorFun<>()
            {
                @Override
                public boolean wrapped()
                {
                    return valueGeneratorFun.wrapped();
                }

                @Override
                public Double next( SplittableRandom rng )
                {
                    return valueGeneratorFun.next( rng ).doubleValue();
                }

                @Override
                public Value nextValue( SplittableRandom rng )
                {
                    return Values.doubleValue( next( rng ) );
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
            NumberToDoubleGeneratorFactory<?> that = (NumberToDoubleGeneratorFactory<?>) o;
            return Objects.equals( valueGeneratorFactory, that.valueGeneratorFactory );
        }

        @Override
        public int hashCode()
        {
            return Objects.hash( valueGeneratorFactory );
        }
    }

    private static class NumberToFloatGeneratorFactory<NUMBER extends Number> implements ValueGeneratorFactory<Float>
    {
        private ValueGeneratorFactory<NUMBER> valueGeneratorFactory;

        private NumberToFloatGeneratorFactory()
        {
        }

        private NumberToFloatGeneratorFactory( ValueGeneratorFactory<NUMBER> valueGeneratorFactory )
        {
            this.valueGeneratorFactory = valueGeneratorFactory;
        }

        @Override
        public ValueGeneratorFun<Float> create()
        {
            ValueGeneratorFun<NUMBER> valueGeneratorFun = valueGeneratorFactory.create();
            return new ValueGeneratorFun<>()
            {
                @Override
                public boolean wrapped()
                {
                    return valueGeneratorFun.wrapped();
                }

                @Override
                public Float next( SplittableRandom rng )
                {
                    return valueGeneratorFun.next( rng ).floatValue();
                }

                @Override
                public Value nextValue( SplittableRandom rng )
                {
                    return Values.floatValue( next( rng ) );
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
            NumberToFloatGeneratorFactory<?> that = (NumberToFloatGeneratorFactory<?>) o;
            return Objects.equals( valueGeneratorFactory, that.valueGeneratorFactory );
        }

        @Override
        public int hashCode()
        {
            return Objects.hash( valueGeneratorFactory );
        }
    }

    private static class RandomByteGeneratorFactory implements ValueGeneratorFactory<Byte>
    {
        private byte min;
        private int range;

        private RandomByteGeneratorFactory()
        {
        }

        private RandomByteGeneratorFactory( byte min, byte max )
        {
            this.min = min;
            this.range = max - min;
        }

        @Override
        public ValueGeneratorFun<Byte> create()
        {
            return new ValueGeneratorFun<>()
            {
                @Override
                public boolean wrapped()
                {
                    return false;
                }

                @Override
                public Byte next( SplittableRandom rng )
                {
                    return (byte) (min + (byte) (rng.nextDouble() * range));
                }

                @Override
                public Value nextValue( SplittableRandom rng )
                {
                    return Values.byteValue( next( rng ) );
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
            RandomByteGeneratorFactory that = (RandomByteGeneratorFactory) o;
            return min == that.min &&
                   range == that.range;
        }

        @Override
        public int hashCode()
        {
            return Objects.hash( min, range );
        }
    }

    private static class RandomShortGeneratorFactory implements ValueGeneratorFactory<Short>
    {
        private short min;
        private int range;

        private RandomShortGeneratorFactory()
        {
        }

        private RandomShortGeneratorFactory( short min, short max )
        {
            this.min = min;
            this.range = max - min;
        }

        @Override
        public ValueGeneratorFun<Short> create()
        {
            return new ValueGeneratorFun<>()
            {
                @Override
                public boolean wrapped()
                {
                    return false;
                }

                @Override
                public Short next( SplittableRandom rng )
                {
                    return (short) (min + (short) (rng.nextDouble() * range));
                }

                @Override
                public Value nextValue( SplittableRandom rng )
                {
                    return Values.shortValue( next( rng ) );
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
            RandomShortGeneratorFactory that = (RandomShortGeneratorFactory) o;
            return min == that.min &&
                   range == that.range;
        }

        @Override
        public int hashCode()
        {
            return Objects.hash( min, range );
        }
    }

    private static class RandomIntegerGeneratorFactory implements ValueGeneratorFactory<Integer>
    {
        private int min;
        private int range;

        private RandomIntegerGeneratorFactory()
        {
        }

        private RandomIntegerGeneratorFactory( int min, int max )
        {
            this.min = min;
            this.range = max - min;
        }

        @Override
        public ValueGeneratorFun<Integer> create()
        {
            return new ValueGeneratorFun<>()
            {
                @Override
                public boolean wrapped()
                {
                    return false;
                }

                @Override
                public Integer next( SplittableRandom rng )
                {
                    return min + (int) (rng.nextDouble() * range);
                }

                @Override
                public Value nextValue( SplittableRandom rng )
                {
                    return Values.intValue( next( rng ) );
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
            RandomIntegerGeneratorFactory that = (RandomIntegerGeneratorFactory) o;
            return min == that.min &&
                   range == that.range;
        }

        @Override
        public int hashCode()
        {
            return Objects.hash( min, range );
        }
    }

    private static class RandomLongGeneratorFactory implements ValueGeneratorFactory<Long>
    {
        private long min;
        private long range;

        private RandomLongGeneratorFactory()
        {
        }

        private RandomLongGeneratorFactory( long min, long max )
        {
            this.min = min;
            this.range = max - min;
        }

        @Override
        public ValueGeneratorFun<Long> create()
        {
            return new ValueGeneratorFun<>()
            {
                @Override
                public boolean wrapped()
                {
                    return false;
                }

                @Override
                public Long next( SplittableRandom rng )
                {
                    return min + (long) (rng.nextDouble() * range);
                }

                @Override
                public Value nextValue( SplittableRandom rng )
                {
                    return Values.longValue( next( rng ) );
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
            RandomLongGeneratorFactory that = (RandomLongGeneratorFactory) o;
            return min == that.min &&
                   range == that.range;
        }

        @Override
        public int hashCode()
        {
            return Objects.hash( min, range );
        }
    }

    private static class RandomFloatGeneratorFactory implements ValueGeneratorFactory<Float>
    {
        private float min;
        private float range;

        private RandomFloatGeneratorFactory()
        {
        }

        private RandomFloatGeneratorFactory( float min, float max )
        {
            this.min = min;
            this.range = max - min;
        }

        @Override
        public ValueGeneratorFun<Float> create()
        {
            return new ValueGeneratorFun<>()
            {
                @Override
                public boolean wrapped()
                {
                    return false;
                }

                @Override
                public Float next( SplittableRandom rng )
                {
                    return min + ((float) rng.nextDouble()) * range;
                }

                @Override
                public Value nextValue( SplittableRandom rng )
                {
                    return Values.floatValue( next( rng ) );
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
            RandomFloatGeneratorFactory that = (RandomFloatGeneratorFactory) o;
            return Float.compare( that.min, min ) == 0 &&
                   Float.compare( that.range, range ) == 0;
        }

        @Override
        public int hashCode()
        {
            return Objects.hash( min, range );
        }
    }

    private static class RandomDoubleGeneratorFactory implements ValueGeneratorFactory<Double>
    {
        private double min;
        private double range;

        private RandomDoubleGeneratorFactory()
        {
        }

        private RandomDoubleGeneratorFactory( double min, double max )
        {
            this.min = min;
            this.range = max - min;
        }

        @Override
        public ValueGeneratorFun<Double> create()
        {
            return new ValueGeneratorFun<>()
            {
                @Override
                public boolean wrapped()
                {
                    return false;
                }

                @Override
                public Double next( SplittableRandom rng )
                {
                    return min + (rng.nextDouble() * range);
                }

                @Override
                public Value nextValue( SplittableRandom rng )
                {
                    return Values.doubleValue( next( rng ) );
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
            RandomDoubleGeneratorFactory that = (RandomDoubleGeneratorFactory) o;
            return Double.compare( that.min, min ) == 0 &&
                   Double.compare( that.range, range ) == 0;
        }

        @Override
        public int hashCode()
        {
            return Objects.hash( min, range );
        }
    }

    private static class AscByteGeneratorFactory implements ValueGeneratorFactory<Byte>
    {
        private byte initialState;

        private AscByteGeneratorFactory()
        {
        }

        private AscByteGeneratorFactory( byte initialState )
        {
            this.initialState = initialState;
        }

        @Override
        public ValueGeneratorFun<Byte> create()
        {
            return new ValueGeneratorFun<>()
            {
                private byte state = initialState;

                @Override
                public boolean wrapped()
                {
                    return false;
                }

                @Override
                public Byte next( SplittableRandom rng )
                {
                    return state++;
                }

                @Override
                public Value nextValue( SplittableRandom rng )
                {
                    return Values.byteValue( next( rng ) );
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
            AscIntegerGeneratorFactory that = (AscIntegerGeneratorFactory) o;
            return initialState == that.initialState;
        }

        @Override
        public int hashCode()
        {
            return Objects.hash( initialState );
        }
    }

    private static class AscShortGeneratorFactory implements ValueGeneratorFactory<Short>
    {
        private short initialState;

        private AscShortGeneratorFactory()
        {
        }

        private AscShortGeneratorFactory( short initialState )
        {
            this.initialState = initialState;
        }

        @Override
        public ValueGeneratorFun<Short> create()
        {
            return new ValueGeneratorFun<>()
            {
                private short state = initialState;

                @Override
                public boolean wrapped()
                {
                    return false;
                }

                @Override
                public Short next( SplittableRandom rng )
                {
                    return state++;
                }

                @Override
                public Value nextValue( SplittableRandom rng )
                {
                    return Values.shortValue( next( rng ) );
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
            AscIntegerGeneratorFactory that = (AscIntegerGeneratorFactory) o;
            return initialState == that.initialState;
        }

        @Override
        public int hashCode()
        {
            return Objects.hash( initialState );
        }
    }

    private static class AscIntegerGeneratorFactory implements ValueGeneratorFactory<Integer>
    {
        private int initialState;

        private AscIntegerGeneratorFactory()
        {
        }

        private AscIntegerGeneratorFactory( int initialState )
        {
            this.initialState = initialState;
        }

        @Override
        public ValueGeneratorFun<Integer> create()
        {
            return new ValueGeneratorFun<>()
            {
                private int state = initialState;

                @Override
                public boolean wrapped()
                {
                    return false;
                }

                @Override
                public Integer next( SplittableRandom rng )
                {
                    return state++;
                }

                @Override
                public Value nextValue( SplittableRandom rng )
                {
                    return Values.intValue( next( rng ) );
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
            AscIntegerGeneratorFactory that = (AscIntegerGeneratorFactory) o;
            return initialState == that.initialState;
        }

        @Override
        public int hashCode()
        {
            return Objects.hash( initialState );
        }
    }

    private static class AscLongGeneratorFactory implements ValueGeneratorFactory<Long>
    {
        private long initialState;

        private AscLongGeneratorFactory()
        {
        }

        private AscLongGeneratorFactory( long initialState )
        {
            this.initialState = initialState;
        }

        @Override
        public ValueGeneratorFun<Long> create()
        {
            return new ValueGeneratorFun<>()
            {
                private long state = initialState;

                @Override
                public boolean wrapped()
                {
                    return false;
                }

                @Override
                public Long next( SplittableRandom rng )
                {
                    return state++;
                }

                @Override
                public Value nextValue( SplittableRandom rng )
                {
                    return Values.longValue( next( rng ) );
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
            AscLongGeneratorFactory that = (AscLongGeneratorFactory) o;
            return initialState == that.initialState;
        }

        @Override
        public int hashCode()
        {
            return Objects.hash( initialState );
        }
    }

    private static class AscFloatGeneratorFactory implements ValueGeneratorFactory<Float>
    {
        private float initialState;

        private AscFloatGeneratorFactory()
        {
        }

        private AscFloatGeneratorFactory( float initialState )
        {
            this.initialState = initialState;
        }

        @Override
        public ValueGeneratorFun<Float> create()
        {
            return new ValueGeneratorFun<>()
            {
                private float state = initialState;

                @Override
                public boolean wrapped()
                {
                    return false;
                }

                @Override
                public Float next( SplittableRandom rng )
                {
                    return state++;
                }

                @Override
                public Value nextValue( SplittableRandom rng )
                {
                    return Values.floatValue( next( rng ) );
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
            AscFloatGeneratorFactory that = (AscFloatGeneratorFactory) o;
            return Float.compare( that.initialState, initialState ) == 0;
        }

        @Override
        public int hashCode()
        {
            return Objects.hash( initialState );
        }
    }

    private static class AscDoubleGeneratorFactory implements ValueGeneratorFactory<Double>
    {
        private double initialState;

        private AscDoubleGeneratorFactory()
        {
        }

        private AscDoubleGeneratorFactory( double initialState )
        {
            this.initialState = initialState;
        }

        @Override
        public ValueGeneratorFun<Double> create()
        {
            return new ValueGeneratorFun<>()
            {
                private double state = initialState;

                @Override
                public boolean wrapped()
                {
                    return false;
                }

                @Override
                public Double next( SplittableRandom rng )
                {
                    return state++;
                }

                @Override
                public Value nextValue( SplittableRandom rng )
                {
                    return Values.doubleValue( next( rng ) );
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
            AscDoubleGeneratorFactory that = (AscDoubleGeneratorFactory) o;
            return Double.compare( that.initialState, initialState ) == 0;
        }

        @Override
        public int hashCode()
        {
            return Objects.hash( initialState );
        }
    }

    /**
     * Creates ID sequence that satisfies the following criteria:
     * <ul>
     * <li>only 1/stride percent of ID space is accessed
     * <li>access is uniformly distributed across ID space --> warms page cache ASAP & spreads load</li>
     * <li>every ID that is returned is returned with equal frequency</li>
     * <li>sequence is in range [0,max)</li>
     * <li>consecutively generated numbers are separated by stride, except when wrapping at max</li>
     * <li>if sliding==true, offset after wrap = (offset before wrap + 1) % stride</li>
     * </ul>
     * <p>
     * When stride = 2, max = 10, offset = 1, sliding = true:
     * </pre>
     * Sequence
     * 1
     * 3
     * 5
     * 7
     * 9
     * 0
     * 2
     * 4
     * 6
     * 8
     * 1       <-- wrap
     * 3
     * </pre>
     * <p>
     * When stride = 2, max = 10, offset = 6, sliding = false:
     * </pre>
     * Sequence
     * 6
     * 8
     * 0
     * 2
     * 4
     * 6       <-- wrap
     * 8
     * </pre>
     */
    private static class StridingNumberGeneratorFactory<NUMBER extends Number> implements ValueGeneratorFactory<NUMBER>
    {
        private int stride;
        private int max;
        private int initial;
        private int baseOffset;
        private boolean sliding;
        private Class<? extends Number> clazz;

        private StridingNumberGeneratorFactory()
        {
        }

        private StridingNumberGeneratorFactory(
                NUMBER stride,
                NUMBER max,
                NUMBER offset,
                boolean sliding )
        {
            if ( stride.intValue() < 1 || stride.intValue() >= max.intValue() )
            {
                throw new RuntimeException( format( "stride (%s) must be between [1,max <%s>)", stride, max ) );
            }
            if ( offset.intValue() < 0 || offset.intValue() >= max.intValue() )
            {
                throw new RuntimeException( format( "offset (%s) must be between [0,max <%s>)", stride, max ) );
            }

            this.stride = stride.intValue();
            this.max = max.intValue();
            this.baseOffset = offset.intValue() % stride.intValue();
            this.initial = offset.intValue();
            this.sliding = sliding;
            this.clazz = offset.getClass();
        }

        private static final Map<Class,IntFunction> TYPE_MAPPING_FUNS = new HashMap<>()
        {
            {
                put( Integer.class, value -> value );
                put( Long.class, value -> (long) value );
                put( Float.class, value -> (float) value );
                put( Double.class, value -> (double) value );
            }
        };

        private static IntFunction typeMappingFunFor( Class<? extends Number> number )
        {
            if ( !TYPE_MAPPING_FUNS.containsKey( number ) )
            {
                throw new RuntimeException( "Unsupported type. Can not map to int" );
            }
            return TYPE_MAPPING_FUNS.get( number );
        }

        @Override
        public ValueGeneratorFun<NUMBER> create()
        {
            final IntFunction<NUMBER> toNumFun = typeMappingFunFor( clazz );
            return new ValueGeneratorFun<>()
            {
                private int offset = baseOffset % stride;
                private int state = initial - stride;
                private int count;

                /**
                 * @return true if entire range was covered and then restarted on previous next call, else false
                 */
                @Override
                public boolean wrapped()
                {
                    return state == initial && count > 1;
                }

                @Override
                public NUMBER next( SplittableRandom rng )
                {
                    count++;
                    state += stride;
                    if ( state >= max )
                    {
                        if ( sliding )
                        {
                            offset = ++offset % stride;
                        }
                        state = offset;
                        return toNumFun.apply( state );
                    }
                    else
                    {
                        return toNumFun.apply( state );
                    }
                }

                @Override
                public Value nextValue( SplittableRandom rng )
                {
                    return Values.numberValue( next( rng ) );
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
            StridingNumberGeneratorFactory<?> that = (StridingNumberGeneratorFactory<?>) o;
            return stride == that.stride &&
                   max == that.max &&
                   initial == that.initial &&
                   baseOffset == that.baseOffset &&
                   sliding == that.sliding &&
                   Objects.equals( clazz, that.clazz );
        }

        @Override
        public int hashCode()
        {
            return Objects.hash( stride, max, initial, baseOffset, sliding, clazz );
        }
    }
}
