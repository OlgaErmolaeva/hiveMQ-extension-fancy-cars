package org.extension.transformer;

import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import com.hivemq.extension.sdk.api.annotations.NotNull;
import com.hivemq.extension.sdk.api.packets.publish.ModifiableOutboundPublish;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.Optional;

public class BackendPayloadTransformer implements PayloadTransformer {

    private static final @NotNull Logger log = LoggerFactory.getLogger(BackendPayloadTransformer.class);

    @Override
    public Optional<String> transformPayload(ModifiableOutboundPublish publishPacket) {
        var maybePayload = publishPacket.getPayload().map(payload -> StandardCharsets.UTF_8.decode(payload).toString());

        if (maybePayload.isPresent()) {
            var payload = maybePayload.get();

            var gson = new Gson();
            BackendResponse backendResponse;

            try {
                backendResponse = gson.fromJson(payload, BackendResponse.class);
            } catch (JsonSyntaxException e) {
                log.error("Exception during parsing response from backend. {}", payload);
                return Optional.empty();
            }

            var command = backendResponse.command == null ? "" : backendResponse.command;
            var subject = backendResponse.subject == null ? "" : backendResponse.subject;

            return Optional.of(String.format("%s %s", command, subject));
        }
        return Optional.empty();
    }

    private static class BackendResponse {
        private String command;
        private String subject;
    }
}
