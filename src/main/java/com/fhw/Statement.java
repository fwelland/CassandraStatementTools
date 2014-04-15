package com.fhw;

import java.io.*;
import java.util.*;
import lombok.*;
@Data
public class Statement
{
    private UUID archivedStatementId; 
    private int customerId;
    private int day;
    private int month;
    private int year;
    private String statementType; 
    private String statementPath;
    
    public UUID getArchivedStatementId()
    {
        if(null == archivedStatementId)
        {
            archivedStatementId = UUID.randomUUID();
        }
        return(archivedStatementId); 
    }
    
    public String getStatementFilename()
    {
        String fname = null; 
        if(null != statementPath)
        {
            File f = new File(statementPath);
            fname = f.getName();
        }
        return(fname); 
    }
}
