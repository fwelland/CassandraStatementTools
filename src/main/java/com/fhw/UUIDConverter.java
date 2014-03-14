package com.fhw;

import com.beust.jcommander.*;
import java.util.UUID;


public class UUIDConverter
    implements IStringConverter<UUID>
{
    @Override
    public UUID convert(String string)
    {
        return ( UUID.fromString(string) );
    }    
}
