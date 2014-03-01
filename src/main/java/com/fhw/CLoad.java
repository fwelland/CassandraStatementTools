package com.fhw;

import com.beust.jcommander.*;
import com.beust.jcommander.converters.*;
import com.datastax.driver.core.*;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Cluster.Builder;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.*;
import static com.datastax.driver.core.querybuilder.QueryBuilder.eq;
import java.io.*;
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
import java.util.*;

public class CLoad
{
    @Parameter(names = "-node", description = "cassandra host:port", converter = HostPortConverter.class, required = true)
    private List<HostPort> nodes = new ArrayList<>();
    
    @Parameter(names = "-date", description = "specfic date to operate on (fmt  yyyy-MM-dd)", required = true)
    private Date statementDate; 
    
    @Parameter(names = "-id", description = "The integral id of the entity performing operation", required = true)
    private Integer entityId;     
    
    @Parameter(names ="-keyspace", description = "key space name to work on (default entitystatements)")
    private String keyspace = "statementarchive";
    
    @Parameter(names ="-table", description = "key space name to work on (default entitystatements)")
    private String table = "statements";
    
    @Parameter(names ="-statementId", description = "(required)The statement id of statement to operate one")
    private String statementId;    
    
    @Parameter(names ="-file", description = "(required)The path & name of the statement file to operate on", converter = FileConverter.class)
    private File statementFile;  
    
    @Parameter(names = "-consistency", description="(required)The consistency level of the cassandra operation", converter = ConsistencyLevelConverter.class)
    private ConsistencyLevel clevel;
    
    private PrintStream pStream = System.out; 
    
    private Cluster cluster;
    private Session session;

    public static void main(String args[])
    {
        String margs[] = new String[]{"-node", "127.0.0.1", "-date", "2014-02-26", "-id", "4799", "-statementId", "9700", "-consistency", "LOCAL_QUORUM", "-file", "/home/fwelland/Downloads/pdf-sample.pdf"};
        CLoad c = new CLoad();
        new JCommander(c, margs);
        c.connect();
        try
        {
            c.addStatement();
        }
        catch(Exception e)
        {
            e.printStackTrace();
        }
        c.close();
        System.exit(0);
    }
    
    private void verbosef(String fmt, Object ... args)
    {
        pStream.printf(fmt, args); 
    }
    
    public void connect()
    {
        Builder bob = Cluster.builder(); 
        for(HostPort node : nodes)
        {
            bob.addContactPoint(node.getHost());
            Integer p = node.getPort(); 
            if(null != p)
                bob.withPort(p);
        }
        cluster = bob.build();
        Metadata metadata = cluster.getMetadata();
        verbosef("Connected to cluster: %s\n", metadata.getClusterName());
        for (Host host : metadata.getAllHosts())
        {
            verbosef("Datatacenter: %s; Host: %s; Rack: %s\n", host.getDatacenter(), host.getAddress(), host.getRack());
        }
        session = cluster.connect();
    }
    
    private void addStatement()
            throws IOException
    {
        Calendar c = Calendar.getInstance();
        c.setTime(statementDate);
        int year = c.get(Calendar.YEAR);
        int day = c.get(Calendar.DAY_OF_MONTH);
        int month = c.get(Calendar.MONTH);
        String fileName = statementFile.getName();
        
        ByteBuffer buffer;
        try (RandomAccessFile aFile = new RandomAccessFile(statementFile, "r"); FileChannel inChannel = aFile.getChannel())
        {
            long fileSize = inChannel.size();
            buffer = ByteBuffer.allocate((int) fileSize);
            inChannel.read(buffer);
            buffer.rewind();
        }            
        Insert i = QueryBuilder.insertInto(keyspace,table);
        i.value("archive_id", UUID.randomUUID());
        i.value("entity_id", entityId);
        i.value("day", day);
        i.value("month", month);
        i.value("year", year); 
        i.value("statement_id", statementId); 
        i.value("statement_filename", fileName);
        i.value("statement", buffer);
        i.setConsistencyLevel(clevel); 
        session.execute(i);        
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
                "INSERT INTO pinappsreportarchive.reports "
                + "(report_archive_id, bank_id, day, description, month, report, report_id, year) "
                + "VALUES (?, ?, ?, ?, ?, ?, ?, ?);");

        BoundStatement boundStatement = new BoundStatement(statement);
        UUID uuid = UUID.randomUUID();
        String path = "/home/fwelland/reports/2010/47900/01/1/B47900R0770.pdf.gz";
        ByteBuffer buffer;
        try (RandomAccessFile aFile = new RandomAccessFile(path, "r"); FileChannel inChannel = aFile.getChannel())
        {
            long fileSize = inChannel.size();
            buffer = ByteBuffer.allocate((int) fileSize);
            inChannel.read(buffer);
            
            
            
            buffer.rewind();
        }
        boundStatement.bind(uuid, 47900, 1, "test report", 1, buffer, "4790", 2014);
        session.execute(boundStatement);
    }

    public void readAFile(String reportUUID)
            throws Exception
    {
        UUID uuid = UUID.fromString(reportUUID);
        Statement q = QueryBuilder.select("report_id", "report").from("pinappsreportarchive", "reports").where(eq("report_archive_id", uuid));
        ResultSet rs = session.execute(q);
        Row r = rs.one();
        if (null != r)
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

        FileVisitor crawler = new SimpleFileVisitor<Path>()
        {
            private int count = 0;

            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attr)
            {
                try
                {
                    addReport(file);
                }
                catch (Exception e)
                {
                    System.out.println("failed adding " + file.toString() + "; error:  " + e.getMessage());
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
        String p = absPath.substring(_root_len + 1);
        String s = p.substring(0, 4);
        year = Integer.parseInt(s);
        s = p.substring(5, 10);
        bank_id = Integer.parseInt(s);
        s = p.substring(11, 13);
        month = Integer.parseInt(s);
        int idx = p.indexOf('/', 14);
        s = p.substring(14, idx);
        day = Integer.parseInt(s);
        String description = reportPath.getFileName().toString();
        idx = description.indexOf('R') + 1;
        int end = description.indexOf('.');
        report_id = description.substring(idx, end);

        PreparedStatement statement = session.prepare(
                "INSERT INTO pinappsreportarchive.reports "
                + "(report_archive_id, bank_id, day, description, month, report, report_id, year) "
                + "VALUES (?, ?, ?, ?, ?, ?, ?, ?);");

        BoundStatement boundStatement = new BoundStatement(statement);
        UUID uuid = UUID.randomUUID();

        ByteBuffer buffer;
        try (RandomAccessFile aFile = new RandomAccessFile(absPath, "r"); FileChannel inChannel = aFile.getChannel())
        {
            long fileSize = inChannel.size();
            buffer = ByteBuffer.allocate((int) fileSize);
            inChannel.read(buffer);
            buffer.rewind();
        }
        boundStatement.bind(uuid, bank_id, day, description, month, buffer, report_id, year);
        session.execute(boundStatement);
    }
    
    public void readSomeRows()
    {
        ResultSet results = session.execute("select * from pinappsreportarchive.reports");
        for (Row row : results)
        {
            UUID uuid = row.getUUID("report_archive_id");
            System.out.println(String.format("%-30s\t%-20s\t%d", uuid.toString(), row.getString("report_id"), row.getInt("bank_id")));
        }
    }
    
}


/**
 * 
create KEYSPACE  statementarchive 
WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 2 };
USE "StatementArchive";
CREATE TABLE statementarchive.statements (         
    archive_id uuid,         
    entity_id int,         
    statement_id text,         
    statement_filename text,         
    year int,         
    month int,         
    day int,         
    statement blob,         
    primary key (archive_id));

* 
* 
 * 
 */