package bigqueryestatespring.configurations;

import bigqueryestatespring.controllers.EstatesController;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import org.jooq.DSLContext;
import org.jooq.SQLDialect;
import org.jooq.impl.DSL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Scope;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;

import static bigqueryestatespring.exceptionMessages.ExceptionMessage.*;

@Configuration
public class EstatesConfiguration {
    private static final Logger logger = LoggerFactory.getLogger(EstatesController.class);

    private static final String pathToCredentials = System.getenv("PATH_TO_GCLOUD_CREDENTIALS");

    /**
     * Gets bigQuery instance with credentials for estates project
     *
     * @return bigQuery instance
     */
    @Bean
    @Scope("singleton")
    public BigQuery getBigQueryInstance() {
        if (pathToCredentials == null) {
            logger.error(PATH_TO_GCLOUD_CREDENTIALS_IS_NOT_SPECIFIED);
            throw new RuntimeException(PATH_TO_GCLOUD_CREDENTIALS_IS_NOT_SPECIFIED);
        }
        try {
            return BigQueryOptions.newBuilder()
                    .setCredentials(
                            ServiceAccountCredentials
                                    .fromStream(new FileInputStream(pathToCredentials))
                    ).build().getService();
        } catch (FileNotFoundException ex) {
            logger.error(CREDENTIALS_FILE_NOT_FOUND);
            throw new RuntimeException(CREDENTIALS_FILE_NOT_FOUND);
        } catch (IOException ex) {
            logger.error(EXCEPTION_WHILE_READING_CREDENTIALS);
            throw new RuntimeException(EXCEPTION_WHILE_READING_CREDENTIALS);
        }
    }

    @Bean
    @Scope("singleton")
    @Qualifier("defaultDslContextConfiguration")
    public DSLContext getDslContext() {
        return DSL.using(SQLDialect.MYSQL);
    }

    @Bean
    @Scope("singleton")
    @Qualifier("defaultObjectMapper")
    public ObjectMapper getObjectMapper() {
        return new ObjectMapper();
    }
}
