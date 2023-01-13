
/*
 * Copyright 2018-present HiveMQ GmbH
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.extension;

import com.hivemq.client.mqtt.MqttGlobalPublishFilter;
import com.hivemq.client.mqtt.mqtt5.Mqtt5BlockingClient;
import com.hivemq.client.mqtt.mqtt5.Mqtt5Client;
import com.hivemq.client.mqtt.mqtt5.message.publish.Mqtt5Publish;
import com.hivemq.extension.sdk.api.annotations.NotNull;
import com.hivemq.testcontainer.core.MavenHiveMQExtensionSupplier;
import com.hivemq.testcontainer.junit5.HiveMQTestContainerExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;

import java.nio.charset.StandardCharsets;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;


@Testcontainers
class FancyCarOutboundInterceptorIT {

    private static final @NotNull Network network = Network.newNetwork();

    @RegisterExtension
    public final @NotNull HiveMQTestContainerExtension extension =
            new HiveMQTestContainerExtension()
                    .withFileInExtensionHomeFolder(MountableFile.forClasspathResource("app.properties"),
                            "hiveMQ-fancy-cars-extension",
                            "")
                    .withExtension(MavenHiveMQExtensionSupplier.direct().get())
                    .withNetwork(network);

    @Container
    private final PostgreSQLContainer<?> postgresqlContainer = new PostgreSQLContainer<>(
            DockerImageName.parse("postgres").withTag("latest"))
            .withNetwork(network)
            .withNetworkAliases("postgres_host")
            .withDatabaseName("hive_mq")
            .withUsername("root")
            .withPassword("root")
            .withInitScript("init_db.sql");

    @Test
    @Timeout(value = 1, unit = TimeUnit.MINUTES)
    void test_payload_from_old_device_modified() throws InterruptedException {
        postgresqlContainer.getPortBindings().add("5432:5432");
        final Mqtt5BlockingClient client = Mqtt5Client.builder()
                .identifier("old-car-12_old_34")
                .serverPort(extension.getMqttPort())
                .buildBlocking();
        client.connect();

        final Mqtt5BlockingClient.Mqtt5Publishes publishes = client.publishes(MqttGlobalPublishFilter.ALL);
        client.subscribeWith().topicFilter("fancy-cars/12_old_34/temperature").send();

        client.publishWith().topic("fancy-cars/12_old_34/temperature").payload("15.0°C".getBytes(StandardCharsets.UTF_8)).send();
        client.publishWith().topic("fancy-cars/12_old_34/temperature").payload("-10.0°F".getBytes(StandardCharsets.UTF_8)).send();
        client.publishWith().topic("fancy-cars/12_old_34/temperature").payload("273.0°K".getBytes(StandardCharsets.UTF_8)).send(); // Kelvin is not defined

        final Mqtt5Publish receive_c = publishes.receive();
        final Mqtt5Publish receive_f = publishes.receive();
        final Mqtt5Publish receive_k = publishes.receive();

        assertTrue(receive_c.getPayload().isPresent());
        assertTrue(receive_f.getPayload().isPresent());
        assertTrue(receive_k.getPayload().isPresent());
        assertEquals("{ \"temperature\": \"15.0\", \"unit\": \"celsius\" }", new String(receive_c.getPayloadAsBytes(), StandardCharsets.UTF_8));
        assertEquals("{ \"temperature\": \"-10.0\", \"unit\": \"fahrenheit\" }", new String(receive_f.getPayloadAsBytes(), StandardCharsets.UTF_8));
        assertEquals("{ \"temperature\": \"273.0\", \"unit\": \"\" }", new String(receive_k.getPayloadAsBytes(), StandardCharsets.UTF_8));
    }

    @Test
    @Timeout(value = 1, unit = TimeUnit.MINUTES)
    void test_payload_from_new_device_not_modified() throws InterruptedException {
        postgresqlContainer.getPortBindings().add("5432:5432");
        final Mqtt5BlockingClient client = Mqtt5Client.builder()
                .identifier("new-car-5678")
                .serverPort(extension.getMqttPort())
                .buildBlocking();
        client.connect();

        final Mqtt5BlockingClient.Mqtt5Publishes publishes = client.publishes(MqttGlobalPublishFilter.ALL);
        client.subscribeWith().topicFilter("fancy-cars/5678/temperature").send();

        client.publishWith().topic("fancy-cars/5678/temperature").payload("{ \"temperature\": \"15.0\", \"unit\": \"fahrenheit\" }".getBytes(StandardCharsets.UTF_8)).send();
        client.publishWith().topic("fancy-cars/5678/temperature").payload("{ \"temperature\": \"0\", \"unit\": \"celsius\" }".getBytes(StandardCharsets.UTF_8)).send();
        client.publishWith().topic("fancy-cars/5678/temperature").payload("{ \"temperature\": \"-5.0\", \"unit\": \"celsius\" }".getBytes(StandardCharsets.UTF_8)).send();

        final Mqtt5Publish receive_15 = publishes.receive();
        final Mqtt5Publish receive_20 = publishes.receive();
        final Mqtt5Publish receive_5 = publishes.receive();

        assertTrue(receive_15.getPayload().isPresent());
        assertTrue(receive_20.getPayload().isPresent());
        assertTrue(receive_5.getPayload().isPresent());
        assertEquals("{ \"temperature\": \"15.0\", \"unit\": \"fahrenheit\" }", new String(receive_15.getPayloadAsBytes(), StandardCharsets.UTF_8));
        assertEquals("{ \"temperature\": \"0\", \"unit\": \"celsius\" }", new String(receive_20.getPayloadAsBytes(), StandardCharsets.UTF_8));
        assertEquals("{ \"temperature\": \"-5.0\", \"unit\": \"celsius\" }", new String(receive_5.getPayloadAsBytes(), StandardCharsets.UTF_8));
    }

    //TODO: Ask how to write a negative test, when message should not be published
    @Test
    @Timeout(value = 1, unit = TimeUnit.MINUTES)
    void test_skip_clientId_is_not_in_DB() throws InterruptedException {
        postgresqlContainer.getPortBindings().add("5432:5432");
        final Mqtt5BlockingClient client = Mqtt5Client.builder()
                .identifier("any_car")
                .serverPort(extension.getMqttPort())
                .buildBlocking();
        client.connect();

        final Mqtt5BlockingClient.Mqtt5Publishes publishes = client.publishes(MqttGlobalPublishFilter.ALL);
        client.subscribeWith().topicFilter("fancy-cars/+/temperature").send();

        client.publishWith().topic("fancy-cars/bla-bla/temperature").payload("whatever-bla-bla".getBytes(StandardCharsets.UTF_8)).send();
        client.publishWith().topic("fancy-cars/5678/temperature").payload("{ \"temperature\": \"-5.0\", \"unit\": \"celsius\" }".getBytes(StandardCharsets.UTF_8)).send();
        client.publishWith().topic("fancy-cars/12_old_34/temperature").payload("11.0°F".getBytes(StandardCharsets.UTF_8)).send();

        // bla-bla should not be published
        final Mqtt5Publish receive_new = publishes.receive();
        final Mqtt5Publish receive_old = publishes.receive();
        assertTrue(receive_new.getPayload().isPresent());
        assertTrue(receive_old.getPayload().isPresent());

        String payload_new = new String(receive_new.getPayloadAsBytes(), StandardCharsets.UTF_8);
        String payload_old = new String(receive_old.getPayloadAsBytes(), StandardCharsets.UTF_8);

        // TODO: Ask if my assumption is correct: when subscribed on wildcard topic, order is not guaranteed, so we can not test in order
        var payloads = Set.of(payload_old, payload_new);

        assertTrue(payloads.contains("{ \"temperature\": \"-5.0\", \"unit\": \"celsius\" }"));
        assertTrue(payloads.contains("{ \"temperature\": \"11.0\", \"unit\": \"fahrenheit\" }"));
    }

    @Test
    @Timeout(value = 1, unit = TimeUnit.MINUTES)
    void test_payload_to_old_device_modified() throws InterruptedException {
        postgresqlContainer.getPortBindings().add("5432:5432");
        final Mqtt5BlockingClient client = Mqtt5Client.builder()
                .identifier("backend")
                .serverPort(extension.getMqttPort())
                .buildBlocking();
        client.connect();

        final Mqtt5BlockingClient.Mqtt5Publishes publishes = client.publishes(MqttGlobalPublishFilter.ALL);
        client.subscribeWith().topicFilter("fancy-cars/12_old_34/command").send();

        client.publishWith().topic("fancy-cars/12_old_34/command").payload("{\"command\": \"open\", \"subject\": \"door\"}".getBytes(StandardCharsets.UTF_8)).send();
        client.publishWith().topic("fancy-cars/12_old_34/command").payload("{\"command\": \"explode\"}".getBytes(StandardCharsets.UTF_8)).send();
        client.publishWith().topic("fancy-cars/12_old_34/command").payload("{\"command\": \"\", \"subject\": \"anything\"}".getBytes(StandardCharsets.UTF_8)).send();

        final Mqtt5Publish receive_open = publishes.receive();
        final Mqtt5Publish receive_explode = publishes.receive();
        final Mqtt5Publish receive_nothing = publishes.receive();

        assertTrue(receive_open.getPayload().isPresent());
        assertTrue(receive_explode.getPayload().isPresent());
        assertTrue(receive_nothing.getPayload().isPresent());
        assertEquals("open door", new String(receive_open.getPayloadAsBytes(), StandardCharsets.UTF_8));
        assertEquals("explode ", new String(receive_explode.getPayloadAsBytes(), StandardCharsets.UTF_8));
        assertEquals(" anything", new String(receive_nothing.getPayloadAsBytes(), StandardCharsets.UTF_8));
    }

}