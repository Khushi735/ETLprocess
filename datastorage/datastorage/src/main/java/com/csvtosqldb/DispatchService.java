package com.csvtosqldb;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;

// GAS MAPPING: This class represents the "Main Worker" section of the script.
@Service 
public class DispatchService {
    // GAS MAPPING: Replaces 'var conn = getConnection(...)'. This is in the application.properties now.
    // Spring Boot manages the connection opening/closing automatically.
    @Autowired
    private JdbcTemplate jdbcTemplate;
    // GAS MAPPING: This is the 'upsertDispatchsheet' function.
    // Instead of DriveApp.getFile, we accept an InputStream (generic file data).
    public void upsertDispatchSheet(InputStream fileInputStream) {
        try {
            // 1. EXTRACT & PARSE (Replaces Utilities.parseCsv)
            InputStreamReader reader = new InputStreamReader(fileInputStream);
            // GAS used a semicolon delimiter ';'.Create the format using the new Builder pattern
            CSVFormat customFormat = CSVFormat.DEFAULT.builder()
            .setDelimiter(';')
            .build();
// Use the new format to create the parser
            CSVParser csvParser = new CSVParser(reader, customFormat);
            csvParser.close(); 
            // 2. LOOKUP DATA (Replaces the SELECT UPPER(type)... block)
            String lookupSql = "SELECT UPPER(type) AS cntr_type, id AS cntr_id FROM summary.dispatch_cntr_mst WHERE status = 'Active'";
            Map<String, Integer> containerTypes = new HashMap<>();
            
            // GAS: while(rs.next()) { containerTypes[...] = ... }
            // JAVA: We map the result set directly to a HashMap.
            jdbcTemplate.query(lookupSql, (rs) -> { //lmbda to process each row
                containerTypes.put(rs.getString("cntr_type"), rs.getInt("cntr_id"));
            });
            // 3. PREPARE SQL (Replaces the massive String sql = ...)
            String sql = getInsertSql(); // See helper method below
            // 4. LOOP & TRANSFORM (Replaces the for loop iterating csvData)
            // 4. LOOP & TRANSFORM (Replacing the GAS loop)
            for (CSVRecord record : csvParser) {
                
                // --- A. DATE TRANSFORMATION (dd-MMM-yyyy) ---
                LocalDate goodsIssueDate = parseDate(record.get(4));
                LocalDate custDelDate = parseDate(record.get(5));
                LocalDate transportDate = parseDate(record.get(30));

                // --- B. PRE-CALCULATIONS (Logic that was scattered in GAS) ---
                
                // 1. Container Lookup
                String rawContainer = record.get(59).toUpperCase();
                Integer containerId = containerTypes.getOrDefault(rawContainer, null);

                // 2. Status Logic (GAS Line 379: Clean '99' or special chars)
                String status = "Active";
                String rawStatus = record.get(40);
                if (rawStatus != null && rawStatus.contains("99")) {
                    status = "Inactive";
                }

                // 3. Truncate SO Created By (GAS Line 365: Limit to 12 chars)
                String soCreated = record.get(34);
                if (soCreated != null && soCreated.length() > 12) {
                    soCreated = soCreated.substring(0, 12);
                }

                // 4. Parent Customer Logic (GAS Line 430)
                String parentCustCode = record.get(63);
                String parentCustName = record.get(64);
                if (parseInt(parentCustCode) <= 0) {
                    parentCustCode = "0";
                    parentCustName = null;
                }

                // 5. Customer Code/Name Logic (GAS Line 468)
                String customerCode = record.get(101);
                String customerName = record.get(102);
                if (customerCode == null || customerCode.trim().isEmpty() || "0".equals(customerCode)) {
                    customerCode = null;
                    customerName = null;
                }

                // 6. Vessel Logic (GAS Line 486)
                String vessel = record.get(114);
                if (vessel == null || vessel.trim().isEmpty()) vessel = null;

                // 7. MG3 Code (GAS Line 240)
                Integer mg3Code = parseInt(record.get(12));
                if (mg3Code == 0) mg3Code = -1;

                // --- C. THE MAPPING (The Giant List) ---
                // This matches the order of '?' in the SQL exactly.
                // --- C. THE MAPPING (The Giant List) ---
                Object[] params = new Object[] {
                    goodsIssueDate,              // 1. goodsIssue
                    goodsIssueDate,              // 2. goodsIssueDate
                    custDelDate,                 // 3. custDelDate
                    parseInt(record.get(6)),     // 4. shipCode
                    record.get(7),               // 5. shipName
                    record.get(8),               // 6. countryKey
                    record.get(9),               // 7. shipCountry
                    parseInt(record.get(10)),    // 8. materialCode
                    record.get(11),              // 9. materialDesc
                    mg3Code,                     // 10. mg3Code
                    record.get(13),              // 11. mg3Desc
                    parseInt(record.get(1)),     // 12. soNo
                    record.get(24),              // 13. interComp
                    record.get(14),              // 14. incoTerm1
                    record.get(15),              // 15. incoTerm2
                    parseInt(record.get(16)),    // 16. mg4Code
                    record.get(17),              // 17. mg4Desc
                    parseDouble(record.get(18)), // 18. qtySalesUOM
                    parseDouble(record.get(21)), // 19. baseQty
                    parseDouble(record.get(22)), // 20. grossWeight
                    record.get(25),              // 21. shipAddress
                    parseInt(record.get(2)),     // 22. itemNo
                    parseInt(record.get(3)),     // 23. dellineNo
                    record.get(24),              // 24. poNo
                    record.get(23),              // 25. labellingDetails
                    record.get(19),              // 26. salesUOM
                    record.get(20),              // 27. baseUOM
                    parseInt(record.get(26)),    // 28. salesOrg
                    parseInt(record.get(27)),    // 29. plantCode
                    parseDouble(record.get(28)), // 30. noPkg
                    record.get(29),              // 31. matUOM
                    transportDate,               // 32. tDate
                    record.get(32),              // 33. ordType
                    record.get(33),              // 34. ordDesc
                    record.get(31),              // 35. gtiSo
                    soCreated,                   // 36. soCreated
                    record.get(35),              // 37. podSAP
                    parseDouble(record.get(36)), // 38. amtDocCur
                    record.get(37),              // 39. docCur
                    parseDouble(record.get(38)), // 40. exRate
                    parseDouble(record.get(39)), // 41. amtLocCur
                    status,                      // 42. status
                    parseDouble(record.get(41)), // 43. open_qty_base_uom
                    parseDouble(record.get(42)), // 44. open_qty_sale_uom
                    parseDouble(record.get(43)), // 45. re_eval_year
                    record.get(44),              // 46. irms_number
                    record.get(45),              // 47. mrms_number
                    parseYyyyMmDd(record.get(47)), // 48. po_date
                    parseYyyyMmDd(record.get(48)), // 49. so_date
                    record.get(52),              // 50. end_cust_code
                    record.get(58),              // 51. bill_of_lading_remarks
                    containerId,                 // 52. sizeOfContainer
                    record.get(62),              // 53. end_cust_name
                    parentCustCode,              // 54. parent_cust_code
                    parentCustName,              // 55. parent_cust_name
                    parseInt(record.get(78)),    // 56. mg1_code
                    record.get(79),              // 57. mg1_desc
                    parseYyyyMmDd(record.get(68)), // 58. cust_req_dt_1
                    parseYyyyMmDd(record.get(69)), // 59. cust_req_dt_2
                    record.get(94),              // 60. cust_req_dt_2_remarks
                    parseYyyyMmDd(record.get(71)), // 61. cust_req_dt_3
                    record.get(95),              // 62. cust_req_dt_3_remarks
                    parseYyyyMmDd(record.get(73)), // 63. our_cmtmnt_dt_1
                    parseYyyyMmDd(record.get(74)), // 64. our_cmtmnt_dt_2
                    parseYyyyMmDd(record.get(75)), // 65. our_cmtmnt_dt_3
                    parseYyyyMmDd(record.get(76)), // 66. committed_ship_dt_1
                    parseYyyyMmDd(record.get(96)), // 67. committed_ship_dt_2
                    parseYyyyMmDd(record.get(97)), // 68. committed_ship_dt_3
                    record.get(88),              // 69. bkgNo
                    parseYyyyMmDd(record.get(89)), // 70. revised_cntr_placement_dt
                    parseYyyyMmDd(record.get(90)), // 71. sailing
                    parseYyyyMmDd(record.get(91)), // 72. etaDesti
                    parseYyyyMmDd(record.get(100)),// 73. revised_dis_dt_plant
                    customerCode,                // 74. customer_code
                    customerName,                // 75. customer_name
                    parseInt(record.get(103)),   // 76. contract_number
                    parseInt(record.get(104)),   // 77. contract_line
                    parseDouble(record.get(105)),// 78. contract_qty
                    record.get(106),             // 79. contract_uom
                    parseYyyyMmDd(record.get(107)),// 80. contract_valid_from
                    parseYyyyMmDd(record.get(108)),// 81. contract_valid_to
                    record.get(109),             // 82. contract_unit_price
                    record.get(110),             // 83. contract_price_uom
                    record.get(111),             // 84. contract_price_unit
                    record.get(112),             // 85. contract_currency
                    vessel,                      // 86. vessel
                    record.get(81),              // 87. batchNo
                    parseYyyyMmDd(record.get(116)),// 88. batch_mfg_date
                    parseYyyyMmDd(record.get(117)),// 89. batch_exp_date
                    record.get(93)               // 90. cust_req_dt_1_remarks
                };

                // Execute the update
                jdbcTemplate.update(sql, params);
            }
            System.out.println("Batch processing complete.");

        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("Error processing CSV: " + e.getMessage());
        }
    }


    // HELPER METHODS (Replacing the bottom functions in GAS)
    // ---------------------------------------------------------

    // GAS: getMonthInd + split logic
    // JAVA: DateTimeFormatter
    private LocalDate parseDate(String dateStr) {
        if (dateStr == null || dateStr.trim().isEmpty()) return null;
        try {
            // Matches format "12-Jan-2023"
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern("dd-MMM-yyyy");
            return LocalDate.parse(dateStr, formatter);
        } catch (Exception e) {
            return null; // Returns SQL NULL
        }
    }

    // GAS: checkValues + parseFloat
    private Double parseDouble(String value) {
        if (value == null || value.contains("-") || value.isEmpty()) return 0.0;
        try {
            return Double.parseDouble(value);
        } catch (NumberFormatException e) {
            return 0.0;
        }
    }

    private Integer parseInt(String value) {
        if (value == null || value.isEmpty()) return 0;
        try {
            return Integer.parseInt(value);
        } catch (NumberFormatException e) {
            return 0;
        }
    }

    // GAS: The massive SQL string at the top of the function
    private String getInsertSql() {
        return "INSERT INTO `summary`.`dispatchsheet` (" +
            // --- 1. COLUMN NAMES ---
            "`goodsIssue`, `goodsIssueDate`, `custDelDate`, `shipCode`, `shipName`, `countryKey`, `shipCountry`, " +
            "`materialCode`, `materialDesc`, `mg3Code`, `mg3Desc`, `soNo`, `interComp`, `incoTerm1`, `incoTerm2`, " +
            "`mg4Code`, `mg4Desc`, `qtySalesUOM`, `baseQty`, `grossWeight`, `shipAddress`, `itemNo`, `dellineNo`, " +
            "`poNo`, `labellingDetails`, `salesUOM`, `baseUOM`, `salesOrg`, `plantCode`, `noPkg`, `matUOM`, `tDate`, " +
            "`ordType`, `ordDesc`, `gtiSo`, `soCreated`, `podSAP`, `amtDocCur`, `docCur`, `exRate`, `amtLocCur`, " +
            "`status`, `open_qty_base_uom`, `open_qty_sale_uom`, `re_eval_year`, `irms_number`, `mrms_number`, " +
            "`po_date`, `so_date`, `end_cust_code`, `bill_of_lading_remarks`, `sizeOfContainer`, `end_cust_name`, " +
            "`parent_cust_code`, `parent_cust_name`, `mg1_code`, `mg1_desc`, `cust_req_dt_1`, `cust_req_dt_2`, " +
            "`cust_req_dt_2_remarks`, `cust_req_dt_3`, `cust_req_dt_3_remarks`, `our_cmtmnt_dt_1`, `our_cmtmnt_dt_2`, " +
            "`our_cmtmnt_dt_3`, `committed_ship_dt_1`, `committed_ship_dt_2`, `committed_ship_dt_3`, `bkgNo`, " +
            "`revised_cntr_placement_dt`, `sailing`, `etaDesti`, `revised_dis_dt_plant`, `customer_code`, " +
            "`customer_name`, `contract_number`, `contract_line`, `contract_qty`, `contract_uom`, " +
            "`contract_valid_from`, `contract_valid_to`, `contract_unit_price`, `contract_price_uom`, " +
            "`contract_price_unit`, `contract_currency`, `vessel`, `batchNo`, `batch_mfg_date`, `batch_exp_date`, " +
            "`cust_req_dt_1_remarks`" +
            
            ") VALUES (" +            "?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, " +
            "?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, " +
            "?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?" +
            
            ") ON DUPLICATE KEY UPDATE " +
            "`goodsIssue`=VALUES(`goodsIssue`), `goodsIssueDate`=VALUES(`goodsIssueDate`), `custDelDate`=VALUES(`custDelDate`), " +
            "`shipCode`=VALUES(`shipCode`), `shipName`=VALUES(`shipName`), `countryKey`=VALUES(`countryKey`), " +
            "`shipCountry`=VALUES(`shipCountry`), `materialCode`=VALUES(`materialCode`), `materialDesc`=VALUES(`materialDesc`), " +
            "`mg3Code`=VALUES(`mg3Code`), `mg3Desc`=VALUES(`mg3Desc`), `interComp`=VALUES(`interComp`), " +
            "`incoTerm1`=VALUES(`incoTerm1`), `incoTerm2`=VALUES(`incoTerm2`), `mg4Code`=VALUES(`mg4Code`), " +
            "`mg4Desc`=VALUES(`mg4Desc`), `qtySalesUOM`=VALUES(`qtySalesUOM`), `baseQty`=VALUES(`baseQty`), " +
            "`grossWeight`=VALUES(`grossWeight`), `shipAddress`=VALUES(`shipAddress`), `dellineNo`=VALUES(`dellineNo`), " +
            "`poNo`=VALUES(`poNo`), `labellingDetails`=VALUES(`labellingDetails`), `salesUOM`=VALUES(`salesUOM`), " +
            "`baseUOM`=VALUES(`baseUOM`), `salesOrg`=VALUES(`salesOrg`), `plantCode`=VALUES(`plantCode`), " +
            "`noPkg`=VALUES(`noPkg`), `matUOM`=VALUES(`matUOM`), `tDate`=VALUES(`tDate`), `ordType`=VALUES(`ordType`), " +
            "`ordDesc`=VALUES(`ordDesc`), `gtiSo`=VALUES(`gtiSo`), `soCreated`=VALUES(`soCreated`), `podSAP`=VALUES(`podSAP`), " +
            "`amtDocCur`=VALUES(`amtDocCur`), `docCur`=VALUES(`docCur`), `exRate`=VALUES(`exRate`), `amtLocCur`=VALUES(`amtLocCur`), " +
            "`status`=VALUES(`status`), `open_qty_base_uom`=VALUES(`open_qty_base_uom`), `open_qty_sale_uom`=VALUES(`open_qty_sale_uom`), " +
            "`re_eval_year`=VALUES(`re_eval_year`), `irms_number`=VALUES(`irms_number`), `mrms_number`=VALUES(`mrms_number`), " +
            "`po_date`=VALUES(`po_date`), `so_date`=VALUES(`so_date`), `end_cust_code`=VALUES(`end_cust_code`), " +
            "`bill_of_lading_remarks`=VALUES(`bill_of_lading_remarks`), `sizeOfContainer`=VALUES(`sizeOfContainer`), " +
            "`end_cust_name`=VALUES(`end_cust_name`), `parent_cust_code`=VALUES(`parent_cust_code`), " +
            "`parent_cust_name`=VALUES(`parent_cust_name`), `mg1_code`=VALUES(`mg1_code`), `mg1_desc`=VALUES(`mg1_desc`), " +
            "`cust_req_dt_1`=VALUES(`cust_req_dt_1`), `cust_req_dt_2`=VALUES(`cust_req_dt_2`), " +
            "`cust_req_dt_2_remarks`=VALUES(`cust_req_dt_2_remarks`), `cust_req_dt_3`=VALUES(`cust_req_dt_3`), " +
            "`cust_req_dt_3_remarks`=VALUES(`cust_req_dt_3_remarks`), `our_cmtmnt_dt_1`=VALUES(`our_cmtmnt_dt_1`), " +
            "`our_cmtmnt_dt_2`=VALUES(`our_cmtmnt_dt_2`), `our_cmtmnt_dt_3`=VALUES(`our_cmtmnt_dt_3`), " +
            "`committed_ship_dt_1`=VALUES(`committed_ship_dt_1`), `committed_ship_dt_2`=VALUES(`committed_ship_dt_2`), " +
            "`committed_ship_dt_3`=VALUES(`committed_ship_dt_3`), `bkgNo`=VALUES(`bkgNo`), " +
            "`revised_cntr_placement_dt`=VALUES(`revised_cntr_placement_dt`), `sailing`=VALUES(`sailing`), " +
            "`etaDesti`=VALUES(`etaDesti`), `revised_dis_dt_plant`=VALUES(`revised_dis_dt_plant`), " +
            "`customer_code`=VALUES(`customer_code`), `customer_name`=VALUES(`customer_name`), " +
            "`contract_number`=VALUES(`contract_number`), `contract_line`=VALUES(`contract_line`), " +
            "`contract_qty`=VALUES(`contract_qty`), `contract_uom`=VALUES(`contract_uom`), " +
            "`contract_valid_from`=VALUES(`contract_valid_from`), `contract_valid_to`=VALUES(`contract_valid_to`), " +
            "`contract_unit_price`=VALUES(`contract_unit_price`), `contract_price_uom`=VALUES(`contract_price_uom`), " +
            "`contract_price_unit`=VALUES(`contract_price_unit`), `contract_currency`=VALUES(`contract_currency`), " +
            "`vessel`=VALUES(`vessel`), `batchNo`=VALUES(`batchNo`), `batch_mfg_date`=VALUES(`batch_mfg_date`), " +
            "`batch_exp_date`=VALUES(`batch_exp_date`), `cust_req_dt_1_remarks`=VALUES(`cust_req_dt_1_remarks`)";
    }
    // This handles the new columns which look like "20230112" (yyyymmdd)
    private LocalDate parseYyyyMmDd(String dateStr) {
        if (dateStr == null || dateStr.trim().isEmpty() || dateStr.equals("0") || dateStr.equals("00000000")) {
            return null;
        }
        try {
            // Uses standard ISO format "20111203"
            return LocalDate.parse(dateStr, DateTimeFormatter.BASIC_ISO_DATE);
        } catch (Exception e) {
            return null;
        }
    }
}