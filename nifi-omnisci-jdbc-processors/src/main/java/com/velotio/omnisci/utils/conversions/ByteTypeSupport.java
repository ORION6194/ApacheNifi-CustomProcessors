package com.velotio.omnisci.utils.conversions;

public class ByteTypeSupport extends TypeSupport<Byte> {
	@Override
	  public Byte convert(Object value) {
	    if (value instanceof Byte) {
	      return (Byte) value;
	    }
	    if (value instanceof String) {
	      return Byte.parseByte((String) value);
	    }
	    if (value instanceof Integer) {
	      return ((Integer)value).byteValue();
	    }
	    if (value instanceof Long) {
	      return ((Long)value).byteValue();
	    }
	    if (value instanceof Short) {
	      return ((Short)value).byteValue();
	    }
	    if (value instanceof Float) {
	      return ((Float)value).byteValue();
	    }
	    if (value instanceof Double) {
	      return ((Double)value).byteValue();
	    }
	    if (value instanceof Number) {
	      return ((Number)value).byteValue();
	    }
	    throw new IllegalArgumentException("");
	  }
}
