package com.csvtosqldb;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;

@RestController
public class DispatchController {

    @Autowired
    private DispatchService dispatchService;

    // This creates a URL: http://localhost:8080/upload-dispatch
    // You can send your CSV file here using Postman.
    @PostMapping("/upload-dispatch")
    public String uploadFile(@RequestParam("file") MultipartFile file) {
        try {
            if (file.isEmpty()) {
                return "Error: Please select a file to upload.";
            }

            // This calls your Service (The code you just finished!)
            dispatchService.upsertDispatchSheet(file.getInputStream());
            
            return "Success! File processed and saved to database.";

        } catch (Exception e) {
            e.printStackTrace();
            return "Error: " + e.getMessage();
        }
    }
}
