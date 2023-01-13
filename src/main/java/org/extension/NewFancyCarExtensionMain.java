package org.extension;

import com.hivemq.extension.sdk.api.ExtensionMain;
import com.hivemq.extension.sdk.api.annotations.NotNull;
import com.hivemq.extension.sdk.api.parameter.*;
import com.hivemq.extension.sdk.api.services.Services;
import org.extension.config.PropertiesHolder;
import org.extension.dao.CarInfoDAO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NewFancyCarExtensionMain implements ExtensionMain {

    private static final @NotNull Logger log = LoggerFactory.getLogger(NewFancyCarExtensionMain.class);

    private final CarInfoDAO carInfoDao;

    public NewFancyCarExtensionMain() {
        this.carInfoDao = new CarInfoDAO();
    }

    @Override
    public void extensionStart(final @NotNull ExtensionStartInput extensionStartInput,
                               final @NotNull ExtensionStartOutput extensionStartOutput) {
        final ExtensionInformation extensionInformation = extensionStartInput.getExtensionInformation();
        log.debug("Starting {}:{}!", extensionInformation.getName(), extensionInformation.getVersion());

        final var extensionHomeFolder = extensionStartInput.getExtensionInformation().getExtensionHomeFolder();
        var propertiesHolder = new PropertiesHolder();

        if (!propertiesHolder.tryToLoadProperties(extensionHomeFolder)) {
            extensionStartOutput.preventExtensionStartup("Exception during reading properties.");
        }

        carInfoDao.init(propertiesHolder);

        try {
            Services.initializerRegistry().setClientInitializer(
                    (initializerInput, clientContext) -> {
                        clientContext.addPublishOutboundInterceptor(new FancyCarOutboundInterceptor(carInfoDao, propertiesHolder));
                    });

        } catch (Exception e) {
            log.error("Exception thrown at Fancy Car Extension start: ", e);
            extensionStartOutput.preventExtensionStartup("Exception thrown at Fancy Car Extension start.");
        }
    }

    @Override
    public void extensionStop(final @NotNull ExtensionStopInput extensionStopInput,
                              final @NotNull ExtensionStopOutput extensionStopOutput) {

        carInfoDao.shutDown();

        final ExtensionInformation extensionInformation = extensionStopInput.getExtensionInformation();
        log.info("Stopped {}:{}", extensionInformation.getName(), extensionInformation.getVersion());
    }

}
