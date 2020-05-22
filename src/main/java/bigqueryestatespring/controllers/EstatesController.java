package bigqueryestatespring.controllers;

import bigqueryestatespring.services.DataService;
import com.fasterxml.jackson.databind.JsonNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Arrays;
import java.util.Optional;

import static bigqueryestatespring.exceptionMessages.ExceptionMessage.ARGUMENTS_ARE_NEGATIVE;
import static bigqueryestatespring.exceptionMessages.ExceptionMessage.TOP_BORDER_UNDER_BOTTOM_BORDER;
import static bigqueryestatespring.services.PropertiesAttribute.*;

@RestController
@RequestMapping("/estates")
public class EstatesController {
    private static final Logger logger = LoggerFactory.getLogger(EstatesController.class);

    private DataService service;

    @Autowired
    private void setService(DataService service) {
        this.service = service;
    }

    @GetMapping
    public ResponseEntity<?> getEstates(@RequestParam(required = false) Integer bottom,
                                        @RequestParam(required = false) Integer top) {
        if ((bottom != null && bottom < 0) || (top != null && top < 0)) {
            return new ResponseEntity<>(new RuntimeException(ARGUMENTS_ARE_NEGATIVE), HttpStatus.BAD_REQUEST);
        }
        if (bottom == null) {
            bottom = 0;
        }
        if (top == null) {
            top = Integer.MAX_VALUE;
        }
        if (top < bottom) {
            return new ResponseEntity<>(new RuntimeException(TOP_BORDER_UNDER_BOTTOM_BORDER), HttpStatus.BAD_REQUEST);
        }
        Optional<JsonNode> result = service.getData(Arrays.asList(OPERATION, PROPERTY_TYPE, COUNTRY_NAME, STATE_NAME),
                PRICE, bottom, top);
        return result.isPresent() ? ResponseEntity.ok().body(result.get()) : new ResponseEntity<>(HttpStatus.NOT_FOUND);
    }

    @ExceptionHandler(Exception.class)
    public final ResponseEntity<Exception> handleAllExceptions(Exception ex) {
        logger.error(ex.getMessage());
        return new ResponseEntity<>(ex, HttpStatus.INTERNAL_SERVER_ERROR);
    }
}
