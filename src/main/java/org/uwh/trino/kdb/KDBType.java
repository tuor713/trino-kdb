package org.uwh.trino.kdb;

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.spi.block.ArrayBlockBuilder;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.*;
import kx.c;

import java.sql.Timestamp;
import java.time.LocalDate;
import java.util.Arrays;
import java.util.TimeZone;
import java.util.UUID;

public enum KDBType {
    Boolean('b', BooleanType.BOOLEAN, (bb, values) -> {
        for (boolean b : (boolean[]) values) {
            BooleanType.BOOLEAN.writeBoolean(bb, b);
        }
    }, (block) -> {
        boolean[] ar = new boolean[block.getPositionCount()];
        for (int i=0; i<block.getPositionCount(); i++) {
            ar[i] = BooleanType.BOOLEAN.getBoolean(block, i);
        }
        return ar;
    }),
    BooleanArray('B', new ArrayType(BooleanType.BOOLEAN), (bb, values) -> { writeArray(KDBType.Boolean, bb, values); }, null),
    GUID('g', UuidType.UUID, (bb, values) -> {
        for (UUID uuid : (UUID[]) values) {
            if (!uuid.equals(c.NULL[2])) {
                Slice slice = Slices.allocate(16);
                slice.setLong(0, uuid.getMostSignificantBits());
                slice.setLong(8, uuid.getLeastSignificantBits());
                UuidType.UUID.writeSlice(bb, slice);
            } else {
                bb.appendNull();
            }
        }
    }, null),
    GUIDArray('G', new ArrayType(UuidType.UUID), (bb, values) -> { writeArray(KDBType.GUID, bb, values); }, null),
    Byte('x', TinyintType.TINYINT, (bb, values) -> {
        for (byte b : (byte[]) values) {
            TinyintType.TINYINT.writeLong(bb, b);
        }
    }, (block) -> {
        byte[] ar = new byte[block.getPositionCount()];
        for (int i=0; i<block.getPositionCount(); i++) {
            ar[i] = (byte) TinyintType.TINYINT.getLong(block, i);
        }
        return ar;
    }),
    ByteArray('X', new ArrayType(TinyintType.TINYINT), (bb, values) -> { writeArray(KDBType.Byte, bb, values); }, null),
    Short('h', SmallintType.SMALLINT, (bb, values) -> {
        for (short s: (short[]) values) {
            if (s == java.lang.Short.MIN_VALUE)  {
                bb.appendNull();
            } else {
                SmallintType.SMALLINT.writeLong(bb, s);
            }
        }
    }, (block) -> {
        short[] ar = new short[block.getPositionCount()];
        for (int i=0; i<block.getPositionCount(); i++) {
            if (block.isNull(i)) {
                ar[i] = java.lang.Short.MIN_VALUE;
            } else {
                ar[i] = (short) SmallintType.SMALLINT.getLong(block, i);
            }
        }
        return ar;
    }),
    ShortArray('H', new ArrayType(SmallintType.SMALLINT), (bb, values) -> { writeArray(KDBType.Short, bb, values); }, null),
    Int('i', IntegerType.INTEGER, (bb, values) -> {
        for (int i : (int[]) values) {
            if (i == Integer.MIN_VALUE) {
                bb.appendNull();
            } else {
                IntegerType.INTEGER.writeLong(bb, i);
            }
        }
    }, (block) -> {
        int[] ar = new int[block.getPositionCount()];
        for (int i=0; i< block.getPositionCount(); i++) {
            if (block.isNull(i)) {
                ar[i] = Integer.MIN_VALUE;
            } else {
                ar[i] = (int) IntegerType.INTEGER.getLong(block, i);
            }
        }
        return ar;
    }),
    IntArray('I', new ArrayType(IntegerType.INTEGER), (bb, values) -> { writeArray(KDBType.Int, bb, values); }, null),
    Long('j', BigintType.BIGINT, (bb, values) -> {
        for (long l : (long[]) values) {
            if (l == java.lang.Long.MIN_VALUE) {
                bb.appendNull();
            } else {
                BigintType.BIGINT.writeLong(bb, l);
            }
        }
    }, (block) -> {
        long[] ar = new long[block.getPositionCount()];
        for (int i=0; i<block.getPositionCount(); i++) {
            if (block.isNull(i)) {
                ar[i] = java.lang.Long.MIN_VALUE;
            } else {
                ar[i] = BigintType.BIGINT.getLong(block, i);
            }
        }
        return ar;
    }),
    LongArray('J', new ArrayType(BigintType.BIGINT), (bb, values) -> { writeArray(KDBType.Long, bb, values); }, null),
    Real('e', DoubleType.DOUBLE, (bb, values) -> {
        for (float f : (float[]) values) {
            if (c.NULL[8].equals(f)) {
                bb.appendNull();
            } else {
                DoubleType.DOUBLE.writeDouble(bb, f);
            }
        }
    }, (block) -> {
        float[] ar = new float[block.getPositionCount()];
        for (int i=0; i<block.getPositionCount(); i++) {
            if (block.isNull(i)) {
                ar[i] = (java.lang.Float) c.NULL[8];
            } else {
                ar[i] = (float) DoubleType.DOUBLE.getDouble(block, i);
            }
        }
        return ar;
    }),
    RealArray('E', new ArrayType(DoubleType.DOUBLE), (bb, values) -> { writeArray(KDBType.Real, bb, values); }, null),
    Float('f', DoubleType.DOUBLE, (bb, values) -> {
        for (double d : (double[]) values) {
            if (Double.isNaN(d)) {
                bb.appendNull();
            } else {
                DoubleType.DOUBLE.writeDouble(bb, d);
            }
        }
    }, (block) -> {
        double[] ar = new double[block.getPositionCount()];
        for (int i=0; i<block.getPositionCount(); i++) {
            if (block.isNull(i)) {
                ar[i] = Double.NaN;
            } else {
                ar[i] = DoubleType.DOUBLE.getDouble(block, i);
            }
        }
        return ar;
    }),
    FloatArray('F', new ArrayType(DoubleType.DOUBLE), (bb, values) -> { writeArray(KDBType.Float, bb, values); }, null),
    Char('c', VarcharType.VARCHAR, (bb, values) -> {
        for (char c : (char[]) values) {
            VarcharType.VARCHAR.writeString(bb, java.lang.String.valueOf(c));
        }
    }, (block) -> {
        char[] ar = new char[block.getPositionCount()];
        for (int i=0; i<block.getPositionCount(); i++) {
            if (block.isNull(i)) {
                ar[i] = (char) c.NULL[10];
            } else {
                ar[i] = ((String) VarcharType.VARCHAR.getObjectValue(null, block, i)).charAt(0);
            }
        }
        return ar;
    }),
    String('C', VarcharType.VARCHAR, (bb, values) -> {
        for (Object s : (Object[]) values) {
            VarcharType.VARCHAR.writeString(bb, new String((char[]) s));
        }
    }, (block) -> {
        Object[] ar = new Object[block.getPositionCount()];
        for (int i=0; i<block.getPositionCount(); i++) {
            ar[i] = ((String) VarcharType.VARCHAR.getObjectValue(null, block, i)).toCharArray();
        }
        return ar;
    }),
    Symbol('s', VarcharType.VARCHAR, (bb, values) -> {
        for (String s : (String[]) values) {
            if (s.equals(c.NULL[11])) {
                bb.appendNull();
            } else {
                VarcharType.VARCHAR.writeString(bb, s);
            }
        }
    }, (block) -> {
        String[] ar = new String[block.getPositionCount()];
        for (int i=0; i<block.getPositionCount(); i++) {
            ar[i] = (String) VarcharType.VARCHAR.getObjectValue(null, block, i);
        }
        return ar;
    }),
    SymbolArray('S', new ArrayType(VarcharType.VARCHAR), (bb, values) -> { writeArray(KDBType.Symbol, bb, values); }, null),
    Timestamp('p', TimestampType.TIMESTAMP_MICROS, (bb, values) -> {
        for (java.sql.Timestamp ts :(java.sql.Timestamp[]) values) {
            if (c.NULL[12].equals(ts)) {
                bb.appendNull();
            } else {
                // Undo KDB timezone adjustment
                TimestampType.TIMESTAMP_MICROS.writeLong(bb, lg(ts.getTime()) * 1000 + ts.getNanos() / 1000);
            }
        }
    }, (block) -> {
        java.sql.Timestamp[] ar = new Timestamp[block.getPositionCount()];
        for (int i=0; i< block.getPositionCount(); i++) {
            if (block.isNull(i)) {
                ar[i] = (java.sql.Timestamp) c.NULL[12];
            } else {
                long l = TimestampType.TIMESTAMP_MICROS.getLong(block, i);
                int nanos = (int) (l % 1000) * 1000;
                long millis = l / 1000;
                millis = invLg(millis);
                ar[i] = new java.sql.Timestamp(millis);
                ar[i].setNanos(nanos);
            }
        }
        return ar;
    }),
    TimestampArray('P', new ArrayType(TimestampType.TIMESTAMP_MICROS), (bb, values) -> { writeArray(KDBType.Timestamp, bb, values); }, null),
    Month('m', VarcharType.createVarcharType(10), (bb, values) -> {
        for (c.Month m : (c.Month[]) values) {
            if (c.NULL[13].equals(m)) {
                bb.appendNull();
            } else {
                VarcharType.VARCHAR.writeString(bb, m.toString());
            }
        }
    }, null),
    MonthArray('M', new ArrayType(VarcharType.createVarcharType(10)), (bb, values) -> { writeArray(KDBType.Month, bb, values); }, null),
    Date('d', DateType.DATE, (bb, values) -> {
        for (java.sql.Date d : (java.sql.Date[]) values) {
            if (d.equals(c.NULL[14])) {
                bb.appendNull();
            } else {
                DateType.DATE.writeLong(bb, LocalDate.of(d.getYear() + 1900, d.getMonth() + 1, d.getDate()).toEpochDay());
            }
        }
    }, (block) -> {
        java.sql.Date[] ar = new java.sql.Date[block.getPositionCount()];
        for (int i=0; i< block.getPositionCount(); i++) {
            if (block.isNull(i)) {
                ar[i] = (java.sql.Date) c.NULL[14];
            } else {
                long epochDay = DateType.DATE.getLong(block, i);
                LocalDate date = LocalDate.ofEpochDay(epochDay);
                ar[i] = new java.sql.Date(date.getYear()-1900, date.getMonthValue()-1, date.getDayOfMonth());
            }
        }
        return ar;
    }),
    DateArray('D', new ArrayType(DateType.DATE), (bb, values) -> { writeArray(KDBType.Date, bb, values); }, null),
    DateTime('z', TimestampType.TIMESTAMP_MILLIS, (bb, values) -> {
        for (java.util.Date ts :(java.util.Date[]) values) {
            if (c.NULL[15].equals(ts)) {
                bb.appendNull();
            } else {
                // Undo KDB timezone adjustment to convert back into UTC 'local' time
                TimestampType.TIMESTAMP_MILLIS.writeLong(bb, lg(ts.getTime()) * 1000);
            }
        }
    }, (block) -> {
        java.util.Date[] ar = new java.util.Date[block.getPositionCount()];
        for (int i=0; i<block.getPositionCount(); i++) {
            if (block.isNull(i)) {
                ar[i] = (java.util.Date) c.NULL[15];
            } else {
                ar[i] = new java.util.Date(invLg(TimestampType.TIMESTAMP_MILLIS.getLong(block, i) / 1000));
            }
        }
        return ar;
    }),
    DateTimeArray('Z', new ArrayType(TimestampType.TIMESTAMP_MILLIS), (bb, values) -> { writeArray(KDBType.DateTime, bb, values); }, null),
    TimeSpan('n', VarcharType.VARCHAR, (bb, values) -> {
        for (c.Timespan ts : (c.Timespan[]) values) {
            if (c.NULL[16].equals(ts)) {
                bb.appendNull();
            } else {
                VarcharType.VARCHAR.writeString(bb, ts.toString());
            }
        }
    }, null),
    TimeSpanArray('N', new ArrayType(VarcharType.VARCHAR), (bb, values) -> { writeArray(KDBType.TimeSpan, bb, values); }, null),
    Minute('u', TimeType.TIME_SECONDS, (bb, values) -> {
        for (c.Minute m : (c.Minute[]) values) {
            if (c.NULL[17].equals(m)) {
                bb.appendNull();
            } else {
                TimeType.TIME_SECONDS.writeLong(bb, (long) m.i * 60_000_000_000_000L);
            }
        }
    }, null),
    MinuteArray('U', new ArrayType(TimeType.TIME_SECONDS), (bb, values) -> { writeArray(KDBType.Minute, bb, values); }, null),
    Second('v', TimeType.TIME_SECONDS, (bb, values) -> {
        for (c.Second s : (c.Second[]) values) {
            if (c.NULL[18].equals(s)) {
                bb.appendNull();
            } else {
                TimeType.TIME_SECONDS.writeLong(bb, (long) s.i * 1_000_000_000_000L);
            }
        }
    }, null),
    SecondArray('V', new ArrayType(TimeType.TIME_SECONDS), (bb, values) -> { writeArray(KDBType.Second, bb, values); }, null),
    Time('t', TimeType.TIME_MILLIS, (bb, values) -> {
        for (java.sql.Time time : (java.sql.Time[]) values) {
            if (c.NULL[19].equals(time)) {
                bb.appendNull();
            } else {
                // undo KDB time zone adjustment
                TimeType.TIME_MILLIS.writeLong(bb, lg(time.getTime()) * 1_000_000_000L);
            }
        }
    }, null),
    TimeArray('T', new ArrayType(TimeType.TIME_MILLIS), (bb, values) -> { writeArray(KDBType.Time, bb, values); }, null),

    Unknown(' ', VarcharType.VARCHAR, (bb, values) -> {
        for (Object o : (Object[]) values) {
            if (o instanceof char[]) {
                VarcharType.VARCHAR.writeString(bb, java.lang.String.valueOf((char[]) o));
            } else {
                VarcharType.VARCHAR.writeString(bb, o.toString());
            }
        }
    }, (block) -> {
        Object[] ar = new Object[block.getPositionCount()];
        for (int i=0; i<block.getPositionCount(); i++) {
            if (block.isNull(i)) {
                ar[i] = new char[0];
            } else {
                ar[i] = ((String) VarcharType.VARCHAR.getObjectValue(null, block, i)).toCharArray();
            }
        }
        return ar;
    });

    @FunctionalInterface
    public interface BlockWriter {
        void write(BlockBuilder bb, Object value);
    }

    @FunctionalInterface
    public interface BlockReader {
        Object read(Block block);
    }

    private final char typeCode;
    private final Type trinoType;
    private final BlockWriter writer;
    private final BlockReader reader;

    KDBType(char typeCode, Type trinoType, BlockWriter writer, BlockReader reader) {
        this.typeCode = typeCode;
        this.trinoType = trinoType;
        this.writer = writer;
        this.reader = reader;
    }

    public char getTypeCode() {
        return typeCode;
    }

    public Type getTrinoType() {
        return trinoType;
    }

    public void writeBlock(BlockBuilder bb, Object values) {
        writer.write(bb, values);
    }

    /**
     * @param block
     * @return Array of values
     */
    public Object readBlock(Block block) {
        if (reader == null) {
            throw new UnsupportedOperationException("Insert is not supported for type "+this);
        }
        return reader.read(block);
    }

    private static final TimeZone tz=TimeZone.getDefault();
    static long getTzOffset(long x){
        return tz.getOffset(x);
    }
    static long lg(long x){
        return x+getTzOffset(x);
    }

    static long invLg(long x) {
        return x-getTzOffset(x-getTzOffset(x));
    }

    private static void writeArray(KDBType inner, BlockBuilder bb, Object values) {
        for (Object ls: (Object[]) values) {
            ((ArrayBlockBuilder) bb).buildEntry(builder -> {
                inner.writeBlock(builder, ls);
            });
        }
    }

    public static KDBType fromTypeCode(char c) {
        return Arrays.stream(KDBType.values()).filter(t -> t.getTypeCode() == c).findFirst().orElseThrow(() -> new UnsupportedOperationException("Type " + c + " is not implemented"));
    }

    public static KDBType fromTrinoType(Type type) {
        if (type == BigintType.BIGINT) {
            return KDBType.Long;
        } else if (type == DoubleType.DOUBLE) {
            return KDBType.Float;
        } else if (type == BooleanType.BOOLEAN) {
            return KDBType.Boolean;
        } else {
            throw new IllegalArgumentException("Type conversion not implemented for "+type);
        }
    }
}
