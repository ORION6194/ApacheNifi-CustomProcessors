package com.velotio.omnisci.utils.conversions;
import java.util.Date;
public class LongTypeSupport extends TypeSupport<Long> {
	 @Override
	  public Long convert(Object value) {
	    if (value instanceof Long) {
	      return (Long) value;
	    }
	    if (value instanceof String) {
	      return Long.parseLong((String) value);
	    }
	    if (value instanceof Short) {
	      return ((Short)value).longValue();
	    }
	    if (value instanceof Integer) {
	      return ((Integer)value).longValue();
	    }
	    if (value instanceof Byte) {
	      return ((Byte)value).longValue();
	    }
	    if (value instanceof Float) {
	      return ((Float)value).longValue();
	    }
	    if (value instanceof Double) {
	      return ((Double)value).longValue();
	    }
	    if (value instanceof Number) {
	      return ((Number)value).longValue();
	    }
	    if (value instanceof Date) {
	      return ((Date)value).getTime();
	    }
	    throw new IllegalArgumentException("");
	  }
}
