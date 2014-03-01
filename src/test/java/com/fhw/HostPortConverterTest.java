package com.fhw;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;


public class HostPortConverterTest
{
    
    public HostPortConverterTest()
    {
    }
    
    @BeforeClass
    public static void setUpClass()
    {
    }
    
    @AfterClass
    public static void tearDownClass()
    {
    }
    
    @Before
    public void setUp()
    {
    }
    
    @After
    public void tearDown()
    {
    }

    @Test
    public void testConvertHostColonPort()
    {
        String string = "www.host.com:8080";
        HostPortConverter instance = new HostPortConverter();
        HostPort result = instance.convert(string);
        assertTrue("ports not the same", result.getPort().equals(8080));
        assertTrue("host not same", result.getHost().equals("www.host.com"));
    }
    
    @Test
    public void testConvertHostNoPort()
    {
        String string = "www.host.com";
        HostPortConverter instance = new HostPortConverter();
        HostPort result = instance.convert(string);
        assertTrue("host not same", result.getHost().equals("www.host.com"));
        System.out.println("port is: " + result.getPort()); 
        assertNull("should be no port", result.getPort());
    }
    
}
