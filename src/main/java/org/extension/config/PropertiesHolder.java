package org.extension.config;

import com.hivemq.extension.sdk.api.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;

public class PropertiesHolder {
    private static final @NotNull Logger log = LoggerFactory.getLogger(PropertiesHolder.class);

    // DB properties
    private static final String JDBC_URL = "url";
    private static final String DRIVER = "driver";
    private static final String USER = "user";
    private static final String PASSWORD = "password";
    private static final String POOL_SIZE = "poolSize";

    // Fancy Cars Outbound Interceptor timeout
    private static final String OUTBOUND_INTERCEPTOR_TIMEOUT = "timeoutInSeconds";

    private Properties properties;

    public boolean tryToLoadProperties(File extensionHomeFolder) {
        final File file = new File(extensionHomeFolder + File.separator + "app.properties");

        try {
            loadProperties(file);
        } catch (IOException e) {
            log.error("Not able to load configuration file '{}'", file.getAbsolutePath());
            return false;
        }
        return true;
    }

    public String getUrl() {
        Object server = properties.get(JDBC_URL);
        return server != null ? server.toString() : "";
    }

    public String getDriver() {
        Object port = properties.get(DRIVER);
        return port != null ? port.toString() : "";
    }

    public String getUser() {
        Object user = properties.get(USER);
        return user != null ? user.toString() : "";
    }

    public String getPassword() {
        Object password = properties.get(PASSWORD);
        return password != null ? password.toString() : "";
    }

    public int getPoolSize() {
        Object poolSize = properties.get(POOL_SIZE);
        return poolSize != null ? Integer.parseInt(poolSize.toString()) : 1;
    }

    public int getInterceptorTimeout() {
        Object timeout = properties.get(OUTBOUND_INTERCEPTOR_TIMEOUT);
        return timeout != null ? Integer.parseInt(timeout.toString()) : 10;
    }

    private void loadProperties(final @NotNull File file) throws IOException {

        try (final FileReader in = new FileReader(file)) {
            properties = new Properties();
            properties.load(in);
        }
    }
}
