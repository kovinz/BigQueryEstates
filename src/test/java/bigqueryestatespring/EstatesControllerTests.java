package bigqueryestatespring;


import bigqueryestatespring.controllers.EstatesController;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.http.MediaType;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;

import static bigqueryestatespring.exceptionMessages.ExceptionMessage.TOP_BORDER_UNDER_BOTTOM_BORDER;
import static bigqueryestatespring.services.PropertiesAttribute.PRICE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@AutoConfigureMockMvc
@ContextConfiguration(classes = {EstatesController.class})
@WebMvcTest
public class EstatesControllerTests {
    private static final String resourceUrl = "/estates";

    @Autowired
    private MockMvc mockMvc;

    @Test
    public void statusOKAndReturnsTreeWithDefaultHeight() throws Exception{
        MvcResult result = mockMvc.perform(MockMvcRequestBuilders.get(resourceUrl)
                .accept(MediaType.APPLICATION_JSON))
                .andExpect(status().isOk())
                .andReturn();

        String json = result.getResponse().getContentAsString();
        ObjectMapper objectMapper = new ObjectMapper();
        JsonNode jsonNode = objectMapper.readTree(json);
        int treeHeight = 5;

        for (int i = 0; i < treeHeight; i++) {
            jsonNode = jsonNode.get("children").get(0);
        }

        jsonNode.get(PRICE).asDouble();
    }

    @Test
    public void throwsExWithMessageTopBorderUnderBottomBorder() throws Exception{
        MvcResult result = mockMvc.perform(MockMvcRequestBuilders.get(resourceUrl + "?bottom=100&top=1")
                .accept(MediaType.APPLICATION_JSON))
                .andExpect(status().isNotFound())
                .andReturn();

        String json = result.getResponse().getContentAsString();
        ObjectMapper objectMapper = new ObjectMapper();
        JsonNode jsonNode = objectMapper.readTree(json);

        assertEquals(jsonNode.get("message").asText(), TOP_BORDER_UNDER_BOTTOM_BORDER);
    }

}
