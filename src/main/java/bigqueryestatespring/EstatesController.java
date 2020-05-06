package bigqueryestatespring;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api")
public class EstatesController {
    @GetMapping("/estates")
    public ResponseEntity<?> getUserDictionary() {
        try {
            return ResponseEntity.ok().body(Service.getEstates());
        } catch (Exception ex) {
            return new ResponseEntity<>(HttpStatus.NOT_FOUND);
        }
    }
}
