package com.velotio.omnisci.utils.conversions;

import java.time.ZonedDateTime;
import java.util.List;
import java.util.Map;
public class StringTypeSupport extends TypeSupport<String> {
	 @Override
	  public String convert(Object value) {
	    if(value instanceof Map || value instanceof List || value instanceof byte[]) {
	      throw new IllegalArgumentException("");
	    }
	    // ZoneDatetime.toString() does not use a standard format which can be parsed.
	    if (value instanceof ZonedDateTime) {
	      return Utils.format((ZonedDateTime) value);
	    }

	    return value.toString();
	  }
}
