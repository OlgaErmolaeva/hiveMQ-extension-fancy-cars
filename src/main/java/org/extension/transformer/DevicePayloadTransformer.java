package org.extension.transformer;

import com.hivemq.extension.sdk.api.annotations.NotNull;
import com.hivemq.extension.sdk.api.packets.publish.ModifiableOutboundPublish;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.Optional;
import java.util.regex.Pattern;

public class DevicePayloadTransformer implements PayloadTransformer {
    private static final @NotNull Logger log = LoggerFactory.getLogger(DevicePayloadTransformer.class);

    private static final String NEW_GEN_TEMPLATE = "{ \"temperature\": \"%s\", \"unit\": \"%s\" }";

    @Override
    public Optional<String> transformPayload(ModifiableOutboundPublish publishPacket) {
        var maybeOriginal = publishPacket.getPayload().map(payload -> StandardCharsets.UTF_8.decode(payload).toString());

        if (maybeOriginal.isPresent()) {
            String original = maybeOriginal.get();
            if (original.contains("°")) {
                String[] parts = original.split(Pattern.quote("°"));
                if (parts.length < 2) {
                    log.error("Wrong message format. Degree sign is not present.");
                    return Optional.empty();
                }
                String temperature = parts[0];
                String units = parts[1];

                return Optional.of(String.format(NEW_GEN_TEMPLATE, temperature, convertUnits(units)));

            } else {
                log.error("Wrong message format. Degree sign is not present.");
                return Optional.empty();
            }
        }
        return Optional.empty();
    }

    private String convertUnits(String units) {
        switch (units) {
            case "C":
                return DegreeUnit.CELSIUS.getName();
            case "F":
                return DegreeUnit.FAHRENHEIT.getName();
            default:
                return "";
        }
    }
}
