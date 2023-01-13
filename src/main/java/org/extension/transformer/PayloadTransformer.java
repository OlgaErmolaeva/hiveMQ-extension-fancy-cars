package org.extension.transformer;

import com.hivemq.extension.sdk.api.packets.publish.ModifiableOutboundPublish;

import java.util.Optional;

public interface PayloadTransformer {

    Optional<String> transformPayload(ModifiableOutboundPublish publishPacket);
}
