package org.extension.matcher;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class TopicMatcher {

    public static final Pattern temperaturePattern = Pattern.compile("(fancy-cars/)(.*)(/temperature)");
    public static final Pattern commandPattern = Pattern.compile("(fancy-cars/)(.*)(/command)");

    public String getClientIfMatchTemperature(String topic) {
        Matcher temperatureMatcher = temperaturePattern.matcher(topic);
        if (temperatureMatcher.matches()) {
            var clientID = temperatureMatcher.group(2);
            return clientID.isBlank() ? null : clientID; // Seems to be an error if topic is fancy-cars//temperature
        }
        return null;
    }

    public String getClientIfMatchCommand(String topic) {
        Matcher commandMatcher = commandPattern.matcher(topic);
        if (commandMatcher.matches()) {
            var clientID = commandMatcher.group(2);
            return clientID.isBlank() ? null : clientID; // Seems to be an error if topic is fancy-cars//command
        }
        return null;
    }
}
