package com.aokolnychyi.mapreduce;

import org.apache.commons.lang.math.NumberUtils;

public class ScoreParser {

    public static boolean isRecordValid(final String record) {
        final String[] recordElements = record.split(" ");
        return recordElements.length == 2 && NumberUtils.isNumber(recordElements[1]);
    }

    public static String getPlayerName(final String record) {
        final String[] recordElements = record.split(" ");
        return recordElements[0];
    }

    public static int getScore(final String record) {
        final String[] recordElements = record.split(" ");
        final String score = recordElements[1];
        return Integer.parseInt(score);
    }
}
