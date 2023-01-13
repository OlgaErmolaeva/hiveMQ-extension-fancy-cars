package org.extension.dao;

import com.hivemq.extension.sdk.api.annotations.NotNull;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.extension.config.PropertiesHolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;

public class CarInfoDAO {

    private static final @NotNull Logger log = LoggerFactory.getLogger(CarInfoDAO.class);
    private static final String SQL_STATEMENT = "SELECT gen FROM generation where clientid = ?";
    public static final String COLUMN_GEN = "gen";

    private HikariDataSource hikariDataSource;

    public void init(PropertiesHolder propertiesHolder) {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(propertiesHolder.getUrl());
        config.setDriverClassName(propertiesHolder.getDriver());
        config.setUsername(propertiesHolder.getUser());
        config.setPassword(propertiesHolder.getPassword());
        config.setMaximumPoolSize(propertiesHolder.getPoolSize());
        hikariDataSource = new HikariDataSource(config);
    }

    public void shutDown() {
        if (hikariDataSource != null) {
            hikariDataSource.close();
        }
    }

    /**
     * Method to look up in DB if the clientID belongs to old or new generation Car.
     * @param clientId value in DB column "clientid" to look for
     * @return
     * -- true if the given client id belongs to old generation car
     * -- false if the given client id belongs to new generation car
     * -- null if the given client id was not found in DB or connection failed
     */
    public Boolean isOldGenerationCar(final String clientId) {
        log.debug("Executing DB query to find if the client is of the old type.");
        try (var connection = hikariDataSource.getConnection();
             final var preparedStatement = connection.prepareStatement(SQL_STATEMENT)) {
            preparedStatement.setString(1, clientId);

            try (var resultSet = preparedStatement.executeQuery()) {
                if (resultSet.next()) {
                    return resultSet.getInt(COLUMN_GEN) == 1;
                }
            } catch (SQLException e) {
                log.error("Exception during executing query.", e);
            }
        } catch (SQLException e) {
            log.error("Exception during connection to DB.", e);
        }
        return null;
    }
}
