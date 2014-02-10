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
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
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
        c.readAFile("6e15e7bb-fdad-4967-add1-c5417afabd4c");
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
   {
        UUID uuid = UUID.fromString(reportUUID); 
        Statement q = QueryBuilder.select("report_id", "report").from("pinappsreportarchive", "reports").where(eq("report_archive_id", uuid));
        ResultSet rs = session.execute(q); 
        Row r = rs.one();
        if(null != r)
        {
            System.out.println("The report is: " + r.getString("report_id")); 
        }
        else
        {
            System.out.println("oh no null!"); 
        }
        
   }
    
}