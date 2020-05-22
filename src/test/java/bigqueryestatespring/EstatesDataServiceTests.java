package bigqueryestatespring;

import bigqueryestatespring.configurations.EstatesConfiguration;
import bigqueryestatespring.services.DataService;
import bigqueryestatespring.services.EstatesService;
import com.fasterxml.jackson.databind.JsonNode;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ContextConfiguration;

import java.util.Arrays;
import java.util.List;

import static bigqueryestatespring.exceptionMessages.ExceptionMessage.AGGREGATE_COLUMN_IS_NULL;
import static bigqueryestatespring.exceptionMessages.ExceptionMessage.TOP_BORDER_UNDER_BOTTOM_BORDER;
import static bigqueryestatespring.services.PropertiesAttribute.*;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ContextConfiguration(classes = {EstatesConfiguration.class, EstatesService.class})
@SpringBootTest
public class EstatesDataServiceTests {
    private DataService service;

    @Autowired
    public void setService(DataService service) {
        this.service = service;
    }

    @Test
    public void getEstatesTestWithCorrectParameters() {
        List<String> columnNames = Arrays.asList(OPERATION, PROPERTY_TYPE, COUNTRY_NAME, STATE_NAME);
        assertThat(service.getData(columnNames, PRICE, 0, Integer.MAX_VALUE), notNullValue());
    }

    @Test
    public void getEstatesTestWithoutColumns() {
        JsonNode jsonNode = service.getData(null, PRICE, 0, Integer.MAX_VALUE).get();

        jsonNode.get(0).get(PRICE).asDouble();
    }

    @Test
    public void getEstatesTestWithOneColumn() {
        List<String> columnNames = Arrays.asList(OPERATION);
        JsonNode jsonNode = service.getData(columnNames, PRICE, 0, Integer.MAX_VALUE).get().get(0);

        for (int i = 0; i < columnNames.size(); i++) {
            jsonNode = jsonNode.get("children").get(0);
        }

        jsonNode.get(PRICE).asDouble();
    }

    @Test
    public void getEstatesTestWithTopUnderBottom() {
        List<String> columnNames = Arrays.asList(OPERATION, PROPERTY_TYPE, COUNTRY_NAME, STATE_NAME);
        Exception exception = assertThrows
                (RuntimeException.class, () -> service.getData(columnNames, PRICE, 1000, 10));
        assertTrue(exception.getMessage().contains(TOP_BORDER_UNDER_BOTTOM_BORDER));
    }

    @Test
    public void getEstatesTestWithoutAggregateColumn() {
        List<String> columnNames = Arrays.asList(OPERATION, PROPERTY_TYPE, COUNTRY_NAME, STATE_NAME);
        Exception exception = assertThrows
                (RuntimeException.class, () -> service.getData(columnNames, null, 10, 100));
        assertTrue(exception.getMessage().contains(AGGREGATE_COLUMN_IS_NULL));
    }

    @Test
    public void getEstatesTestPriceExists() {
        List<String> columnNames = Arrays.asList(OPERATION, PROPERTY_TYPE, COUNTRY_NAME, STATE_NAME);
        JsonNode jsonNode = service.getData(columnNames, PRICE, 0, Integer.MAX_VALUE).get().get(0);

        for (int i = 0; i < columnNames.size(); i++) {
            jsonNode = jsonNode.get("children").get(0);
        }

        jsonNode.get(PRICE).asDouble();
    }
}
