package com.velotio.omnisci.utils.conversions;

public class ShortTypeSupport extends TypeSupport<Short> {
	@Override
	  public Short convert(Object value) {
	    if (value instanceof Short) {
	      return (Short) value;
	    }
	    if (value instanceof String) {
	      return Short.parseShort((String) value);
	    }
	    if (value instanceof Integer) {
	      return ((Integer)value).shortValue();
	    }
	    if (value instanceof Long) {
	      return ((Long)value).shortValue();
	    }
	    if (value instanceof Byte) {
	      return ((Byte)value).shortValue();
	    }
	    if (value instanceof Float) {
	      return ((Float)value).shortValue();
	    }
	    if (value instanceof Double) {
	      return ((Double)value).shortValue();
	    }
	    if (value instanceof Number) {
	      return ((Number)value).shortValue();
	    }
	    throw new IllegalArgumentException("");
	  }
}
