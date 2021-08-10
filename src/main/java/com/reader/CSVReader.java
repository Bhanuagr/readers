package com.reader;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Paths;

public class CSVReader {
  private static final long customerIdReplacementFlagMask = (0x1L << 59);
  private static final long fragmentMsgIdFlagMask = (0x1L << 60);

  public static void main(String[] args) throws Exception {
    if(args.length == 0){
      throw new Exception("Please pass absolute file name as first parameter");
    }
    String SAMPLE_CSV_FILE_PATH = args[0];
    String header = "userId,entityId,creationTime,MLTBRemoveTime,SenderReceivedTime,sender,mask,txtLen,mlogTimestamp,status,msgStatus,gatewayConfigId,smsType,numberType,causeId,internalId,externalId,priorityCost,interConnectType,err,stat,subDate,doneDate,msgPriority,MSGID,numSMSPerMsg,globalErrorCode,abbid,is_rescheduled,deliveryRetryCount,isFinalReport,isRetryEnabled,credits,nrc,gsc,globalStatus,homeOperatorId,idt,dbc,mtid,htid,dltid,peid";

    try (
            Reader reader = Files.newBufferedReader(Paths.get(SAMPLE_CSV_FILE_PATH));
            CSVParser csvParser = new CSVParser(reader, CSVFormat.DEFAULT.withFirstRecordAsHeader().withIgnoreHeaderCase().withTrim());
    ) {
      for (CSVRecord csvRecord : csvParser) {
        String internalIdString = csvRecord.get("internalId");
        if(isNumeric(internalIdString)) {
          long internalId = Long.parseLong(internalIdString);
          if(!isCustomerIdReplacement(internalId) && isFragmentMsgId(internalId)) {
            System.out.println(internalId);
          }
        }
        else {
          System.out.println("not a numeric: "+ internalIdString);
        }
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
