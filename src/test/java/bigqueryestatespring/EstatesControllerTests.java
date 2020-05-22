package bigqueryestatespring;


import bigqueryestatespring.configurations.EstatesConfiguration;
import bigqueryestatespring.controllers.EstatesController;
import bigqueryestatespring.services.EstatesService;
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
@ContextConfiguration(classes = {EstatesConfiguration.class, EstatesService.class, EstatesController.class})
@WebMvcTest
public class EstatesControllerTests {
    private static final String resourceUrl = "/estates";
    private static ObjectMapper mapper;

    @Autowired
    private void setMapper(ObjectMapper objectMapper) {
        mapper = objectMapper;
    }

    @Autowired
    private MockMvc mockMvc;

    @Test
    public void statusOKAndReturnsTreeWithDefaultHeight() throws Exception{
        MvcResult result = mockMvc.perform(MockMvcRequestBuilders.get(resourceUrl)
                .accept(MediaType.APPLICATION_JSON))
                .andExpect(status().isOk())
                .andReturn();

        String json = result.getResponse().getContentAsString();
        JsonNode jsonNode = mapper.readTree(json).get(0);
        int treeHeight = 4;

        for (int i = 0; i < treeHeight; i++) {
            jsonNode = jsonNode.get("children").get(0);
        }

        jsonNode.get(PRICE).asDouble();
    }

    @Test
    public void throwsExWithMessageTopBorderUnderBottomBorder() throws Exception{
        MvcResult result = mockMvc.perform(MockMvcRequestBuilders.get(resourceUrl + "?bottom=100&top=1")
                .accept(MediaType.APPLICATION_JSON))
                .andExpect(status().isBadRequest())
                .andReturn();

        String json = result.getResponse().getContentAsString();
        JsonNode jsonNode = mapper.readTree(json);

        assertEquals(jsonNode.get("message").asText(), TOP_BORDER_UNDER_BOTTOM_BORDER);
    }

    @Test
    public void getEmptyResult() throws Exception{
        MvcResult result = mockMvc.perform(MockMvcRequestBuilders.get(resourceUrl + "?top=0")
                .accept(MediaType.APPLICATION_JSON))
                .andExpect(status().isNotFound())
                .andReturn();

        String json = result.getResponse().getContentAsString();

        assertEquals(json, "");
    }

}
