package org.extension;

import com.hivemq.extension.sdk.api.annotations.NotNull;
import com.hivemq.extension.sdk.api.async.Async;
import com.hivemq.extension.sdk.api.interceptor.publish.PublishOutboundInterceptor;
import com.hivemq.extension.sdk.api.interceptor.publish.parameter.PublishOutboundInput;
import com.hivemq.extension.sdk.api.interceptor.publish.parameter.PublishOutboundOutput;
import com.hivemq.extension.sdk.api.services.ManagedExtensionExecutorService;
import com.hivemq.extension.sdk.api.services.Services;
import org.extension.config.PropertiesHolder;
import org.extension.dao.CarInfoDAO;
import org.extension.matcher.TopicMatcher;
import org.extension.transformer.BackendPayloadTransformer;
import org.extension.transformer.DevicePayloadTransformer;
import org.extension.transformer.PayloadTransformer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;

public class FancyCarOutboundInterceptor implements PublishOutboundInterceptor {
    private static final @NotNull Logger log = LoggerFactory.getLogger(FancyCarOutboundInterceptor.class);

    private final TopicMatcher topicMatcher;
    private final CarInfoDAO carInfoDAO;
    private final PropertiesHolder propertiesHolder;
    private final BackendPayloadTransformer backendPayloadTransformer;
    private final DevicePayloadTransformer devicePayloadTransformer;

    public FancyCarOutboundInterceptor(CarInfoDAO carInfoDAO, PropertiesHolder propertiesHolder) {
        log.debug("Creating FancyCarOutboundInterceptor");

        this.topicMatcher = new TopicMatcher();
        this.carInfoDAO = carInfoDAO;
        this.propertiesHolder = propertiesHolder;
        this.backendPayloadTransformer = new BackendPayloadTransformer();
        this.devicePayloadTransformer = new DevicePayloadTransformer();
    }

    @Override
    //TODO: Ask if inbound interceptor would be better in this case
    public void onOutboundPublish(@NotNull PublishOutboundInput input,
                                  @NotNull PublishOutboundOutput output) {

        final ManagedExtensionExecutorService extensionExecutorService = Services.extensionExecutorService();

        var topic = input.getPublishPacket().getTopic();

        final var temperatureClientID = topicMatcher.getClientIfMatchTemperature(topic);
        if (temperatureClientID != null) {
            log.debug(String.format("Got a message in temperature topic from %s", temperatureClientID));

            final Async<PublishOutboundOutput> async = output.async(
                    Duration.ofSeconds(propertiesHolder.getInterceptorTimeout()));
            extensionExecutorService.submit(() -> {
                transformPayloadFormatIfNeeded(output, devicePayloadTransformer, temperatureClientID);
                async.resume();
            });
        } else {
            final var commandClientID = topicMatcher.getClientIfMatchCommand(topic);

            if (commandClientID != null) {
                log.debug(String.format("Got a message in command topic from %s", commandClientID));

                final Async<PublishOutboundOutput> async = output.async(
                        Duration.ofSeconds(propertiesHolder.getInterceptorTimeout()));
                extensionExecutorService.submit(() -> {
                    transformPayloadFormatIfNeeded(output, backendPayloadTransformer, commandClientID);
                    async.resume();
                });
            }
        }
    }

    private void transformPayloadFormatIfNeeded(PublishOutboundOutput output,
                                                PayloadTransformer transformer, String clientID) {

        Boolean isOldGen = carInfoDAO.isOldGenerationCar(clientID);

        if (isOldGen == null) { // Error occurs during DB connection
            output.preventPublishDelivery();
        } else if (isOldGen) {
            log.debug("Transforming outbound message for clientID {}", clientID);

            final var publishPacket = output.getPublishPacket();
            var newPayload = transformer.transformPayload(output.getPublishPacket());

            if (newPayload.isEmpty()) {
                output.preventPublishDelivery();
                return;
            }

            final ByteBuffer payload = ByteBuffer.wrap(newPayload.get().getBytes(StandardCharsets.UTF_8));
            publishPacket.setPayload(payload);
        }
    }
}