package com.reader;

import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;

public class ZipReader {
  private static final long customerIdReplacementFlagMask = (0x1L << 59);
  private static final long fragmentMsgIdFlagMask = (0x1L << 60);

  public static void main(String[] args) throws Exception {

    if(args.length == 0){
      throw new Exception("Please pass absolute file name as first parameter");
    }

    if(!args[0].endsWith("tar.gz")){
      throw new Exception("Please provide file with extension as tar.gz");
    }

    try(TarArchiveInputStream tarInput =
                new TarArchiveInputStream(new GzipCompressorInputStream(new FileInputStream(args[0])));
        BufferedReader br = new BufferedReader((new InputStreamReader(tarInput))) // Read directly on the fly
    ) {

      TarArchiveEntry currentEntry = tarInput.getNextTarEntry();
      while (currentEntry != null) {
        System.out.println("For File = " + currentEntry.getName());

        CSVParser csvParser = new CSVParser(br, CSVFormat.DEFAULT.withFirstRecordAsHeader().withIgnoreHeaderCase().withTrim());
        for (CSVRecord csvRecord : csvParser) {
          String internalIdString = csvRecord.get("internalId");
          System.out.println("Processing internalId : " + internalIdString);

          if (isNumeric(internalIdString)) {
            long internalId = Long.parseLong(internalIdString);
            if (!isCustomerIdReplacement(internalId) && isFragmentMsgId(internalId)) {
              System.out.println("Found this internalId with required condition : " + internalId);
            }
          } else {
            System.out.println("Found this internalId is not a numeric : " + internalIdString);
          }
        }

        currentEntry = tarInput.getNextTarEntry(); // going to next entry
      }
    }
  }

  public static boolean isCustomerIdReplacement(long msgId)
  {
    return ((msgId & customerIdReplacementFlagMask) > 0L);
  }

  public static boolean isFragmentMsgId(long msgId)
  {
    return ((msgId & fragmentMsgIdFlagMask) > 0L);
  }

  public static boolean isNumeric(String strNum) {
    if (strNum == null) {
      return false;
    }
    try {
      Long.parseLong(strNum);
    } catch (NumberFormatException nfe) {
      return false;
    }
    return true;
  }
}
