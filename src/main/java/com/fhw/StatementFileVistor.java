package com.fhw;

import java.nio.file.*;
import static java.nio.file.FileVisitResult.CONTINUE;
import java.nio.file.attribute.*;
import lombok.*;

@Data
public class StatementFileVistor
    extends SimpleFileVisitor<Path>
{
    private CLoad loader;
    private int rootLen;
    private int count = 0; 
    
    public StatementFileVistor()
    {

    }
    
    @Override
    public FileVisitResult visitFile(Path file, BasicFileAttributes attr)
    {
        try
        {
            if(attr.isRegularFile())
            {
                loader.addStatement(makeStatement(file)); 
                count++;
            }
        }
        catch (Exception e)
        {
            System.out.println("failed adding " + file.toString() + "; error:  " + e.getMessage());
        }
        return CONTINUE;
    }
    
    private Statement makeStatement(Path file)
    {
        int customerId;
        int year;
        int day;
        int month;
        String statementType;

        String absPath = file.toAbsolutePath().toString();
        String p = absPath.substring(rootLen + 1);
        String s = p.substring(0, 4);
        year = Integer.parseInt(s);
        s = p.substring(5, 10);
        customerId = Integer.parseInt(s);
        s = p.substring(11, 13);
        month = Integer.parseInt(s);
        int idx = p.indexOf('/', 14);
        s = p.substring(14, idx);
        day = Integer.parseInt(s);
        String fileName = file.getFileName().toString();
        idx = fileName.indexOf('R') + 1;
        int end = fileName.indexOf('.');
        statementType = fileName.substring(idx, end);
        Statement stmt = new Statement();
        stmt.setCustomerId(customerId);
        stmt.setDay(day);
        stmt.setMonth(month);
        stmt.setYear(year);
        stmt.setStatementType(statementType);
        stmt.setStatementPath(file.toAbsolutePath().toString());
        return(stmt); 
    }
}