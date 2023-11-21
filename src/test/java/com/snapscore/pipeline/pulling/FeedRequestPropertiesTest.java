package com.snapscore.pipeline.pulling;

import org.junit.Test;

import java.util.Optional;

import static org.junit.Assert.*;

public class FeedRequestPropertiesTest {

    // some example types
    enum FeedRequestPropertyTypes {
        FILE_NAME,
        TEAM_ID
    }

    @Test
    public void basic() {

        FeedRequestProperties properties = new FeedRequestProperties();

        String fileName = "some/file.txt";
        int teamId = 155;
        properties.putProperty(FeedRequestPropertyTypes.FILE_NAME, fileName);
        properties.putProperty(FeedRequestPropertyTypes.TEAM_ID, teamId);

        Optional<String> fileNameVal = properties.getPropertyValue(FeedRequestPropertyTypes.FILE_NAME, String.class);
        assertTrue(fileNameVal.isPresent());
        assertEquals(fileName, fileNameVal.get());

        Optional<Integer> teamIdVal = properties.getPropertyValue(FeedRequestPropertyTypes.TEAM_ID, Integer.class);
        assertTrue(teamIdVal.isPresent());
        assertTrue(teamId == teamIdVal.get());

    }


}
