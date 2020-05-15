package bigqueryestatespring;

import bigqueryestatespring.services.OperationType;
import bigqueryestatespring.services.EstatesService;
import bigqueryestatespring.services.Service;
import com.fasterxml.jackson.databind.JsonNode;
import org.junit.jupiter.api.Test;

import static bigqueryestatespring.exceptionMessages.ExceptionMessage.NOT_GIVEN_COLUMN_NAMES;
import static bigqueryestatespring.exceptionMessages.ExceptionMessage.TOP_BORDER_UNDER_BOTTOM_BORDER;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import static bigqueryestatespring.services.PropertiesAttribute.*;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class EstatesServiceTests {
    @Test
    public void getEstatesTestWithCorrectParameters() {
        Service service = new EstatesService();
        assertThat(service.getData(0, Integer.MAX_VALUE), notNullValue());
    }

    @Test
    public void getEstatesTestWithoutColumns() {
        Service service = new EstatesService(null, OperationType.AVERAGE);
        JsonNode jsonNode = service.getData(0, Integer.MAX_VALUE).get();

        jsonNode.get("children").get(0).get(PRICE).asDouble();
    }

    @Test
    public void getEstatesTestWithOneColumn() {
        List<String> columnNames = Arrays.asList(OPERATION);
        Service service = new EstatesService(columnNames, OperationType.AVERAGE);
        JsonNode jsonNode = service.getData(0, Integer.MAX_VALUE).get();

        for (int i = 0; i <= columnNames.size(); i++) {
            jsonNode = jsonNode.get("children").get(0);
        }

        jsonNode.get(PRICE).asDouble();
    }

    @Test
    public void getEstatesTestWithTopUnderBottom() {
        Service service = new EstatesService();
        Exception exception = assertThrows(RuntimeException.class, () -> service.getData(1000, 10));
        assertTrue(exception.getMessage().contains(TOP_BORDER_UNDER_BOTTOM_BORDER));
    }

    @Test
    public void getEstatesTestPriceExists() {
        List<String> columnNames = Arrays.asList(OPERATION, PROPERTY_TYPE, COUNTRY_NAME, STATE_NAME);
        Service service = new EstatesService(columnNames, OperationType.AVERAGE);
        JsonNode jsonNode = service.getData(0, Integer.MAX_VALUE).get();

        for (int i = 0; i <= columnNames.size(); i++) {
            jsonNode = jsonNode.get("children").get(0);
        }

        jsonNode.get(PRICE).asDouble();
    }
}
