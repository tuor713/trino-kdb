package org.uwh.trino.kdb;

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.*;
import io.trino.type.UuidType;
import kx.c;

import java.time.LocalDate;
import java.util.Arrays;
import java.util.UUID;

public enum KDBType {
    Boolean('b', BooleanType.BOOLEAN, (bb, values) -> {
        for (boolean b : (boolean[]) values) {
            BooleanType.BOOLEAN.writeBoolean(bb, b);
        }
    }),
    GUID('g', UuidType.UUID, (bb, values) -> {
        for (UUID uuid : (UUID[]) values) {
            Slice slice = Slices.allocate(16);
            slice.setLong(0, uuid.getMostSignificantBits());
            slice.setLong(8, uuid.getLeastSignificantBits());
            UuidType.UUID.writeSlice(bb, slice);
        }
    }),
    Byte('x', TinyintType.TINYINT, (bb, values) -> {
        for (byte b : (byte[]) values) {
            TinyintType.TINYINT.writeLong(bb, b);
        }
    }),
    Short('h', SmallintType.SMALLINT, (bb, values) -> {
        for (short s: (short[]) values) {
            SmallintType.SMALLINT.writeLong(bb, s);
        }
    }),
    Int('i', IntegerType.INTEGER, (bb, values) -> {
        for (int i : (int[]) values) {
            IntegerType.INTEGER.writeLong(bb, i);
        }
    }),
    Long('j', BigintType.BIGINT, (bb, values) -> {
        for (long l : (long[]) values) {
            BigintType.BIGINT.writeLong(bb, l);
        }
    }),
    Real('e', DoubleType.DOUBLE, (bb, values) -> {
        for (float f : (float[]) values) {
            DoubleType.DOUBLE.writeDouble(bb, f);
        }
    }),
    Float('f', DoubleType.DOUBLE, (bb, values) -> {
        for (double d : (double[]) values) {
            DoubleType.DOUBLE.writeDouble(bb, d);
        }
    }),
    Char('c', Constants.CHAR_TYPE, (bb, values) -> {
        for (char c : (char[]) values) {
            Constants.CHAR_TYPE.writeString(bb, java.lang.String.valueOf(c));
        }
    }),
    String('C', VarcharType.VARCHAR, (bb, values) -> {
        for (Object s : (Object[]) values) {
            VarcharType.VARCHAR.writeString(bb, new String((char[]) s));
        }
    }),
    Symbol('s', VarcharType.VARCHAR, (bb, values) -> {
        for (String s : (String[]) values) {
            VarcharType.VARCHAR.writeString(bb, s);
        }
    }),
    Timestamp('p', TimestampType.TIMESTAMP_MICROS, (bb, values) -> {
        for (java.sql.Timestamp ts :(java.sql.Timestamp[]) values) {
            TimestampType.TIMESTAMP_MICROS.writeLong(bb,ts.getTime()*1000 + ts.getNanos()/1000);
        }
    }),
    Month('m', VarcharType.createVarcharType(10), (bb, values) -> {
        for (c.Month m : (c.Month[]) values) {
            VarcharType.VARCHAR.writeString(bb, m.toString());
        }
    }),
    Date('d', DateType.DATE, (bb, values) -> {
        for (java.sql.Date d : (java.sql.Date[]) values) {
            DateType.DATE.writeLong(bb, LocalDate.of(d.getYear()+1900, d.getMonth()+1, d.getDate()).toEpochDay());
        }
    }),
    DateTime('z', TimestampType.TIMESTAMP_MILLIS, (bb, values) -> {
        for (java.util.Date ts :(java.util.Date[]) values) {
            TimestampType.TIMESTAMP_MILLIS.writeLong(bb, ts.getTime()*1000);
        }
    }),
    TimeSpan('n', VarcharType.VARCHAR, (bb, values) -> {
        for (c.Timespan ts : (c.Timespan[]) values) {
            VarcharType.VARCHAR.writeString(bb, ts.toString());
        }
    }),
    Minute('u', TimeType.TIME_SECONDS, (bb, values) -> {
        for (c.Minute m : (c.Minute[]) values) {
            TimeType.TIME_SECONDS.writeLong(bb, (long) m.i * 60_000_000_000_000L);
        }
    }),
    Second('v', TimeType.TIME_SECONDS, (bb, values) -> {
        for (c.Second s : (c.Second[]) values) {
            TimeType.TIME_SECONDS.writeLong(bb, (long) s.i * 1_000_000_000_000L);
        }
    }),
    Time('t', TimeType.TIME_MILLIS, (bb, values) -> {
        for (java.sql.Time time : (java.sql.Time[]) values) {
            TimeType.TIME_MILLIS.writeLong(bb, time.getTime() * 1_000_000_000L);
        }
    });

    @FunctionalInterface
    public static interface BlockWriter {
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

    public static KDBType fromTypeCode(char c) {
        return Arrays.stream(KDBType.values()).filter(t -> t.getTypeCode() == c).findFirst().get();
    }
}
