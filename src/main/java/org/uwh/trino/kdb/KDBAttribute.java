package org.uwh.trino.kdb;

import java.util.Arrays;

/**
 * Column attributes in KDB `u`s`g`p
 */
public enum KDBAttribute {
    Grouped('g'),
    Parted('p'),
    Sorted('s'),
    Unique('u');

    private final char code;
    KDBAttribute(char code) {
        this.code = code;
    }

    public char getCode() {
        return code;
    }

    public static KDBAttribute fromCode(char code) {
        return Arrays.stream(KDBAttribute.values()).filter(t -> t.getCode() == code).findFirst().orElseThrow(() -> new UnsupportedOperationException("Attribute " + code + " does not exist."));
    }
}
