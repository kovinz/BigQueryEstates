package bigqueryestatespring.controllers;

import bigqueryestatespring.services.OperationType;
import bigqueryestatespring.services.EstatesService;
import bigqueryestatespring.services.Service;
import com.fasterxml.jackson.databind.JsonNode;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Optional;

import static bigqueryestatespring.exceptionMessages.ExceptionMessage.TOP_BORDER_UNDER_BOTTOM_BORDER;
import static bigqueryestatespring.services.PropertiesAttribute.*;

@RestController
@RequestMapping("/estates")
public class EstatesController {

    @GetMapping
    public ResponseEntity<?> getEstates(@RequestParam(required = false) Integer bottom,
                                        @RequestParam(required = false) Integer top) {
        if (bottom == null) {
            bottom = 0;
        }
        if (top == null) {
            top = Integer.MAX_VALUE;
        }
        if (top < bottom) {
            return new ResponseEntity<>(new RuntimeException(TOP_BORDER_UNDER_BOTTOM_BORDER), HttpStatus.NOT_FOUND);
        }
        Service service = new EstatesService();
        Optional<JsonNode> result = service.getData(bottom, top);
        return result.isPresent() ? ResponseEntity.ok().body(result.get()) : new ResponseEntity<>(HttpStatus.NO_CONTENT);
    }

    @ExceptionHandler(RuntimeException.class)
    public final ResponseEntity<Exception> handleAllExceptions(RuntimeException ex) {
        return new ResponseEntity<>(ex, HttpStatus.INTERNAL_SERVER_ERROR);
    }
}
