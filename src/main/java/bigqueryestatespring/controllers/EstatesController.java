package bigqueryestatespring.controllers;

import bigqueryestatespring.services.OperationType;
import bigqueryestatespring.services.Service;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

import static bigqueryestatespring.services.PropertiesAttribute.*;

@RestController
public class EstatesController {

    @GetMapping("/estates")
    public ResponseEntity<?> getUserDictionary(@RequestParam(required = false) Integer bottom,
                                               @RequestParam(required = false) Integer top) {
        if (bottom == null) {
            bottom = 0;
        }
        if (top == null) {
            top = Integer.MAX_VALUE;
        }
        try {
            Service service = new Service(
                    List.of(OPERATION, PROPERTY_TYPE, COUNTRY_NAME, STATE_NAME,  PRICE),
                    OperationType.AVERAGE);
            Object result = service.getEstates(bottom, top);
            if (result != null) {
                return ResponseEntity.ok().body(result);
            } else {
                return new ResponseEntity<>(HttpStatus.NOT_FOUND);
            }
        } catch (RuntimeException ex) {
            return new ResponseEntity<>(ex.getMessage(), HttpStatus.NOT_FOUND);
        } catch (Exception ex) {
            ex.printStackTrace();
            return new ResponseEntity<>(HttpStatus.NOT_FOUND);
        }
    }
}
