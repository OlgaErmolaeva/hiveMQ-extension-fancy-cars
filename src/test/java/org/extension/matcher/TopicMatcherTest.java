package org.extension.matcher;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

public class TopicMatcherTest {

    private final TopicMatcher matcherUnderTest = new TopicMatcher();

    @ParameterizedTest
    @CsvSource(ignoreLeadingAndTrailingWhitespace = false, value = {
            "fancy-cars/1234/temperature,1234",
            "fancy-cars/device/temperature,device",
            "fancy-cars/?/temperature,?",
            "fancy-cars/hi space/temperature,hi space"
    })
    public void matchTemperatureTopic(String topic, String clientID) {

        Assertions.assertEquals(clientID, matcherUnderTest.getClientIfMatchTemperature(topic));
    }

    @ParameterizedTest
    @CsvSource(ignoreLeadingAndTrailingWhitespace = false, value = {
            "fancy-cars/999/command,999",
            "fancy-cars/justClient/command,justClient",
            "fancy-cars/$%&/command,$%&",
            "fancy-cars/client with space/command,client with space"
    })
    public void matchCommandTopic(String topic, String clientID) {

        Assertions.assertEquals(clientID, matcherUnderTest.getClientIfMatchCommand(topic));
    }

    @ParameterizedTest
    @ValueSource(strings = {"fancy-cars//temperature", "fancy-cars/   /temperature"})
    public void emptyTemperatureTopicToNull(String topic) {

        Assertions.assertNull(matcherUnderTest.getClientIfMatchTemperature(topic));
    }

    @ParameterizedTest
    @ValueSource(strings = {"fancy-cars//command", "fancy-cars/   /command"})
    public void emptyCommandTopicToNull(String topic) {

        Assertions.assertNull(matcherUnderTest.getClientIfMatchCommand(topic));
    }

    @ParameterizedTest
    @ValueSource(strings = {"fancy-monsters/normal_client/command", "fancy-cars/good_client/kill/:)"})
    public void unknownTopicToNull(String topic) {

        Assertions.assertNull(matcherUnderTest.getClientIfMatchCommand(topic));
        Assertions.assertNull(matcherUnderTest.getClientIfMatchTemperature(topic));
    }
}
