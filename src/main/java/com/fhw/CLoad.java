package com.fhw;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import static com.datastax.driver.core.querybuilder.QueryBuilder.eq;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.FileVisitResult;
import static java.nio.file.FileVisitResult.CONTINUE;
import java.nio.file.FileVisitor;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.UUID;

public class CLoad {

    private Cluster cluster;
    private Session session;

    public static void main(String args[]) 
    {
        System.out.println("go");
        CLoad c = new CLoad();
        c.connect("localhost");
        //c.readSomeRows();
//        try
//        {
//            c.insertAFile();
//        }
//        catch(Exception  e)
//        {
//            e.printStackTrace();
//        }
//        try
//        {
//            c.readAFile("6e15e7bb-fdad-4967-add1-c5417afabd4c");
//        }
//        catch(Exception e)
//        {
//            e.printStackTrace();
//        }
        
        try
        {
            long beg = System.currentTimeMillis();
            c.loadReports();
            long end = System.currentTimeMillis();
            System.out.println("total elapse time in ms:  " + (end - beg) ); 
        }
        catch(Exception e)
        {
            e.printStackTrace();
        }
        
        c.close();
        System.exit(0);
    }

    public void connect(String node)
    {
        cluster = Cluster.builder().addContactPoint(node).build();
        Metadata metadata = cluster.getMetadata();
        System.out.printf("Connected to cluster: %s\n", metadata.getClusterName());
        for (Host host : metadata.getAllHosts())
        {
            System.out.printf("Datatacenter: %s; Host: %s; Rack: %s\n", host.getDatacenter(), host.getAddress(), host.getRack());
        }
        session = cluster.connect();
    }
    
    public void readSomeRows()
    {        
        ResultSet results = session.execute("select * from pinappsreportarchive.reports");
        for (Row row : results)
        {
            UUID uuid = row.getUUID("report_archive_id"); 
            System.out.println(String.format("%-30s\t%-20s\t%d", uuid.toString(),row.getString("report_id"),row.getInt("bank_id")));
        }        
    }
    
   public void close()
   {
        session.shutdown();
        cluster.shutdown();
   }
   
   public void insertAFile()
           throws Exception
   {
        PreparedStatement statement = session.prepare(
            "INSERT INTO pinappsreportarchive.reports " +
            "(report_archive_id, bank_id, day, description, month, report, report_id, year) " +
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?);");       
       
        BoundStatement boundStatement = new BoundStatement(statement); 
        UUID uuid = UUID.randomUUID();
        String path = "/home/fwelland/reports/2010/47900/01/1/B47900R0770.pdf.gz"; 
        ByteBuffer buffer; 
        try (RandomAccessFile aFile = new RandomAccessFile(path,"r"); FileChannel inChannel = aFile.getChannel())
        {
            long fileSize = inChannel.size();
            buffer = ByteBuffer.allocate((int) fileSize);
            inChannel.read(buffer);
            buffer.rewind();
        }                                                
        boundStatement.bind(uuid, 47900, 1, "test report", 1, buffer, "4790", 2014 );
        session.execute(boundStatement);                
   }
   
   public void readAFile(String reportUUID)
           throws Exception
   {
        UUID uuid = UUID.fromString(reportUUID); 
        Statement q = QueryBuilder.select("report_id", "report").from("pinappsreportarchive", "reports").where(eq("report_archive_id", uuid));
        ResultSet rs = session.execute(q); 
        Row r = rs.one();
        if(null != r)
        {
            System.out.println("The report is: " + r.getString("report_id")); 
            ByteBuffer buffer = r.getBytes("report");             
            File file = new File("/tmp/foo.dat");
            FileChannel channel = new FileOutputStream(file, false).getChannel();             
            channel.write(buffer); 
            channel.close();
        }
        else
        {
            System.out.println("oh no null!"); 
        }        
   }    
   
   
   public void loadReports()
           throws IOException
   {
       
       FileVisitor  crawler = new SimpleFileVisitor<Path>()
       {       
           private int  count = 0; 
            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attr)
            {
                try
                {
                    addReport(file); 
                }
                catch(Exception e)
                {
                    System.out.println("failed adding " + file.toString() + "; error:  " +e.getMessage()); 
                }
//                count ++;
//                if(count > 3)
//                    System.exit(0); 
                return CONTINUE;
            }
       }; 
       Files.walkFileTree(Paths.get(_root), crawler);       
   }
   
   private static final String _root = "/home/fwelland/reports";
   private static final int _root_len = _root.length(); 
   
   public void addReport(Path reportPath)
           throws IOException
   {
       
       int bank_id; 
       int year; 
       int day; 
       int month; 
       String report_id; 

       String absPath = reportPath.toAbsolutePath().toString(); 
       String p = absPath.substring(_root_len+1); 
       String s = p.substring(0, 4); 
       year = Integer.parseInt(s); 
       s = p.substring(5,10); 
       bank_id = Integer.parseInt(s); 
       s = p.substring(11,13); 
       month = Integer.parseInt(s); 
       int idx = p.indexOf('/',14); 
       s = p.substring(14,idx );
       day = Integer.parseInt(s); 
       String description = reportPath.getFileName().toString();
       idx = description.indexOf('R') + 1;
       int end = description.indexOf('.'); 
       report_id = description.substring(idx,end); 
       
        PreparedStatement statement = session.prepare(
            "INSERT INTO pinappsreportarchive.reports " +
            "(report_archive_id, bank_id, day, description, month, report, report_id, year) " +
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?);");       
       
        BoundStatement boundStatement = new BoundStatement(statement); 
        UUID uuid = UUID.randomUUID();
       
        ByteBuffer buffer; 
        try (RandomAccessFile aFile = new RandomAccessFile(absPath,"r"); FileChannel inChannel = aFile.getChannel())
        {
            long fileSize = inChannel.size();
            buffer = ByteBuffer.allocate((int) fileSize);
            inChannel.read(buffer);
            buffer.rewind();
        }                                                
        boundStatement.bind(uuid, bank_id, day, description, month, buffer, report_id, year );
        session.execute(boundStatement);                       
   }
}