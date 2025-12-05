package com.csvtosqldb;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;

@RestController
public class DispatchController {

    @Autowired
    private DispatchService dispatchService;

    // OPTION 1: Hardcoded Path (Change this to your actual file location)
    // Note: Use double backslashes "\\" for Windows paths
    private final String MY_LOCAL_PATH = "C:/Users/gtiplpt1/Desktop/GTIPL_DISPATCHSHEET.CSV";
    // Trigger via: http://localhost:8080/run-local
    @GetMapping("/run-local")
    public String runLocalFixed() {
        return processLocalFile(MY_LOCAL_PATH);
    }

    // Helper method to handle the logic for both
    private String processLocalFile(String pathString) {
        try {
            // 1. Locate the file on your hard drive
            File file = new File(MY_LOCAL_PATH);

            // 2. Safety Check: Does it exist?
            if (!file.exists()) {
                return "Error: File not found at: " + MY_LOCAL_PATH;
            }

            // 3. Open the stream
            InputStream inputStream = new FileInputStream(file);

            // 4. Send to your Service
            // (Make sure your Service is accepting 'InputStream' as the argument)
            dispatchService.upsertDispatchSheet(inputStream);

            // 5. Clean up
            inputStream.close();

            return "Success! Processed file from: " + MY_LOCAL_PATH;

        } catch (Exception e) {
            e.printStackTrace();
            return "Error: " + e.getMessage();
        }
    }
}
// package com.csvtosqldb;
// import org.springframework.beans.factory.annotation.Autowired;
// import org.springframework.web.bind.annotation.PostMapping;
// import org.springframework.web.bind.annotation.RequestParam;
// import org.springframework.web.bind.annotation.RestController;
// import org.springframework.web.multipart.MultipartFile;

// @RestController
// public class DispatchController {

//     @Autowired
//     private DispatchService dispatchService;

//     // This creates a URL: http://localhost:8080/upload-dispatch
//     // You can send your CSV file here using Postman.
//     @PostMapping("/upload-dispatch")
//     public String uploadFile(@RequestParam("file") MultipartFile file) {
//         try {
//             if (file.isEmpty()) {
//                 return "Error: Please select a file to upload.";
//             }

//             // This calls your Service (The code you just finished!)
//             dispatchService.upsertDispatchSheet(file.getInputStream());
            
//             return "Success! File processed and saved to database.";

//         } catch (Exception e) {
//             e.printStackTrace();
//             return "Error: " + e.getMessage();
//         }
//     }
// }