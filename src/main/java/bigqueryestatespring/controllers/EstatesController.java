package bigqueryestatespring.controllers;

import bigqueryestatespring.services.Service;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

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
            Object result = Service.getEstates(bottom, top);
            return ResponseEntity.ok().body(result);
        } catch (Exception ex) {
            return new ResponseEntity<>(ex.getMessage(), HttpStatus.NOT_FOUND);
        }
    }
}
