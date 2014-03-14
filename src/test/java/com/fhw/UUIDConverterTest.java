package com.fhw;

import java.util.*;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;

public class UUIDConverterTest
{
    
    public UUIDConverterTest()
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
    public void testInOutUUID()
    {
        String string;
        UUIDConverter instance = new UUIDConverter();
        UUID expResult = UUID.randomUUID();
        string = expResult.toString();
        UUID result = instance.convert(string);
        assertEquals(expResult, result);
    }
    
    
}
