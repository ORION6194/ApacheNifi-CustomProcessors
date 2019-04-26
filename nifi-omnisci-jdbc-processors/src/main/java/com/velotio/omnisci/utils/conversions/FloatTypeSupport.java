package com.velotio.omnisci.utils.conversions;

public class FloatTypeSupport  extends TypeSupport<Float> {
	@Override
	  public Float convert(Object value) {
	    if (value instanceof Float) {
	      return (Float) value;
	    }
	    if (value instanceof String) {
	      return Float.parseFloat((String) value);
	    }
	    if (value instanceof Short) {
	      return ((Short)value).floatValue();
	    }
	    if (value instanceof Integer) {
	      return ((Integer)value).floatValue();
	    }
	    if (value instanceof Byte) {
	      return ((Byte)value).floatValue();
	    }
	    if (value instanceof Long) {
	      return ((Long)value).floatValue();
	    }
	    if (value instanceof Double) {
	      return ((Double)value).floatValue();
	    }
	    if (value instanceof Number) {
	      return ((Number)value).floatValue();
	    }
	    throw new IllegalArgumentException("");
	  }
}
