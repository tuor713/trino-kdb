package org.uwh.trino.kdb;

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.*;
import kx.c;

import java.time.LocalDate;
import java.util.Arrays;
import java.util.TimeZone;
import java.util.UUID;

public enum KDBType {
    Boolean('b', BooleanType.BOOLEAN, (bb, values) -> {
        for (boolean b : (boolean[]) values) {
            BooleanType.BOOLEAN.writeBoolean(bb, b);
        }
    }),
    BooleanArray('B', new ArrayType(BooleanType.BOOLEAN), (bb, values) -> { writeArray(KDBType.Boolean, bb, values); }),
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
    }),
    GUIDArray('G', new ArrayType(UuidType.UUID), (bb, values) -> { writeArray(KDBType.GUID, bb, values); }),
    Byte('x', TinyintType.TINYINT, (bb, values) -> {
        for (byte b : (byte[]) values) {
            TinyintType.TINYINT.writeLong(bb, b);
        }
    }),
    ByteArray('X', new ArrayType(TinyintType.TINYINT), (bb, values) -> { writeArray(KDBType.Byte, bb, values); }),
    Short('h', SmallintType.SMALLINT, (bb, values) -> {
        for (short s: (short[]) values) {
            if (s == java.lang.Short.MIN_VALUE)  {
                bb.appendNull();
            } else {
                SmallintType.SMALLINT.writeLong(bb, s);
            }
        }
    }),
    ShortArray('H', new ArrayType(SmallintType.SMALLINT), (bb, values) -> { writeArray(KDBType.Short, bb, values); }),
    Int('i', IntegerType.INTEGER, (bb, values) -> {
        for (int i : (int[]) values) {
            if (i == Integer.MIN_VALUE) {
                bb.appendNull();
            } else {
                IntegerType.INTEGER.writeLong(bb, i);
            }
        }
    }),
    IntArray('I', new ArrayType(IntegerType.INTEGER), (bb, values) -> { writeArray(KDBType.Int, bb, values); }),
    Long('j', BigintType.BIGINT, (bb, values) -> {
        for (long l : (long[]) values) {
            if (l == java.lang.Long.MIN_VALUE) {
                bb.appendNull();
            } else {
                BigintType.BIGINT.writeLong(bb, l);
            }
        }
    }),
    LongArray('J', new ArrayType(BigintType.BIGINT), (bb, values) -> { writeArray(KDBType.Long, bb, values); }),
    Real('e', DoubleType.DOUBLE, (bb, values) -> {
        for (float f : (float[]) values) {
            if (c.NULL[8].equals(f)) {
                bb.appendNull();
            } else {
                DoubleType.DOUBLE.writeDouble(bb, f);
            }
        }
    }),
    RealArray('E', new ArrayType(DoubleType.DOUBLE), (bb, values) -> { writeArray(KDBType.Real, bb, values); }),
    Float('f', DoubleType.DOUBLE, (bb, values) -> {
        for (double d : (double[]) values) {
            if (Double.isNaN(d)) {
                bb.appendNull();
            } else {
                DoubleType.DOUBLE.writeDouble(bb, d);
            }
        }
    }),
    FloatArray('F', new ArrayType(DoubleType.DOUBLE), (bb, values) -> { writeArray(KDBType.Float, bb, values); }),
    Char('c', VarcharType.VARCHAR, (bb, values) -> {
        for (char c : (char[]) values) {
            VarcharType.VARCHAR.writeString(bb, java.lang.String.valueOf(c));
        }
    }),
    String('C', VarcharType.VARCHAR, (bb, values) -> {
        for (Object s : (Object[]) values) {
            VarcharType.VARCHAR.writeString(bb, new String((char[]) s));
        }
    }),
    Symbol('s', VarcharType.VARCHAR, (bb, values) -> {
        for (String s : (String[]) values) {
            if (s.equals(c.NULL[11])) {
                bb.appendNull();
            } else {
                VarcharType.VARCHAR.writeString(bb, s);
            }
        }
    }),
    SymbolArray('S', new ArrayType(VarcharType.VARCHAR), (bb, values) -> { writeArray(KDBType.Symbol, bb, values); }),
    Timestamp('p', TimestampType.TIMESTAMP_MICROS, (bb, values) -> {
        for (java.sql.Timestamp ts :(java.sql.Timestamp[]) values) {
            if (c.NULL[12].equals(ts)) {
                bb.appendNull();
            } else {
                // Undo KDB timezone adjustment
                TimestampType.TIMESTAMP_MICROS.writeLong(bb, lg(ts.getTime()) * 1000 + ts.getNanos() / 1000);
            }
        }
    }),
    TimestampArray('P', new ArrayType(TimestampType.TIMESTAMP_MICROS), (bb, values) -> { writeArray(KDBType.Timestamp, bb, values); }),
    Month('m', VarcharType.createVarcharType(10), (bb, values) -> {
        for (c.Month m : (c.Month[]) values) {
            if (c.NULL[13].equals(m)) {
                bb.appendNull();
            } else {
                VarcharType.VARCHAR.writeString(bb, m.toString());
            }
        }
    }),
    MonthArray('M', new ArrayType(VarcharType.createVarcharType(10)), (bb, values) -> { writeArray(KDBType.Month, bb, values); }),
    Date('d', DateType.DATE, (bb, values) -> {
        for (java.sql.Date d : (java.sql.Date[]) values) {
            if (d.equals(c.NULL[14])) {
                bb.appendNull();
            } else {
                DateType.DATE.writeLong(bb, LocalDate.of(d.getYear() + 1900, d.getMonth() + 1, d.getDate()).toEpochDay());
            }
        }
    }),
    DateArray('D', new ArrayType(DateType.DATE), (bb, values) -> { writeArray(KDBType.Date, bb, values); }),
    DateTime('z', TimestampType.TIMESTAMP_MILLIS, (bb, values) -> {
        for (java.util.Date ts :(java.util.Date[]) values) {
            if (c.NULL[15].equals(ts)) {
                bb.appendNull();
            } else {
                // Undo KDB timezone adjustment to convert back into UTC 'local' time
                TimestampType.TIMESTAMP_MILLIS.writeLong(bb, lg(ts.getTime()) * 1000);
            }
        }
    }),
    DateTimeArray('Z', new ArrayType(TimestampType.TIMESTAMP_MILLIS), (bb, values) -> { writeArray(KDBType.DateTime, bb, values); }),
    TimeSpan('n', VarcharType.VARCHAR, (bb, values) -> {
        for (c.Timespan ts : (c.Timespan[]) values) {
            if (c.NULL[16].equals(ts)) {
                bb.appendNull();
            } else {
                VarcharType.VARCHAR.writeString(bb, ts.toString());
            }
        }
    }),
    TimeSpanArray('N', new ArrayType(VarcharType.VARCHAR), (bb, values) -> { writeArray(KDBType.TimeSpan, bb, values); }),
    Minute('u', TimeType.TIME_SECONDS, (bb, values) -> {
        for (c.Minute m : (c.Minute[]) values) {
            if (c.NULL[17].equals(m)) {
                bb.appendNull();
            } else {
                TimeType.TIME_SECONDS.writeLong(bb, (long) m.i * 60_000_000_000_000L);
            }
        }
    }),
    MinuteArray('U', new ArrayType(TimeType.TIME_SECONDS), (bb, values) -> { writeArray(KDBType.Minute, bb, values); }),
    Second('v', TimeType.TIME_SECONDS, (bb, values) -> {
        for (c.Second s : (c.Second[]) values) {
            if (c.NULL[18].equals(s)) {
                bb.appendNull();
            } else {
                TimeType.TIME_SECONDS.writeLong(bb, (long) s.i * 1_000_000_000_000L);
            }
        }
    }),
    SecondArray('V', new ArrayType(TimeType.TIME_SECONDS), (bb, values) -> { writeArray(KDBType.Second, bb, values); }),
    Time('t', TimeType.TIME_MILLIS, (bb, values) -> {
        for (java.sql.Time time : (java.sql.Time[]) values) {
            if (c.NULL[19].equals(time)) {
                bb.appendNull();
            } else {
                // undo KDB time zone adjustment
                TimeType.TIME_MILLIS.writeLong(bb, lg(time.getTime()) * 1_000_000_000L);
            }
        }
    }),
    TimeArray('T', new ArrayType(TimeType.TIME_MILLIS), (bb, values) -> { writeArray(KDBType.Time, bb, values); }),

    Unknown(' ', VarcharType.VARCHAR, (bb, values) -> {
        for (Object o : (Object[]) values) {
            VarcharType.VARCHAR.writeString(bb, o.toString());
        }
    });

    @FunctionalInterface
    public interface BlockWriter {
        void write(BlockBuilder bb, Object value);
    }

    private final char typeCode;
    private final Type trinoType;
    private final BlockWriter writer;

    KDBType(char typeCode, Type trinoType, BlockWriter writer) {
        this.typeCode = typeCode;
        this.trinoType = trinoType;
        this.writer = writer;
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

    private static final TimeZone tz=TimeZone.getDefault();
    static long getTzOffset(long x){
        return tz.getOffset(x);
    }
    static long lg(long x){
        return x+getTzOffset(x);
    }

    private static void writeArray(KDBType inner, BlockBuilder bb, Object values) {
        for (Object ls: (Object[]) values) {
            BlockBuilder sub = bb.beginBlockEntry();
            inner.writeBlock(sub, ls);
            bb.closeEntry();
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
