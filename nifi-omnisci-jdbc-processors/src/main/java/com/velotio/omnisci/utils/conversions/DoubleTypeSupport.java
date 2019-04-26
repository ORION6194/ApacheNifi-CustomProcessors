package com.velotio.omnisci.utils.conversions;

public class DoubleTypeSupport extends TypeSupport<Double> {
	@Override
	  public Double convert(Object value) {
	    if (value instanceof Double) {
	      return (Double) value;
	    }
	    if (value instanceof String) {
	      return Double.parseDouble((String) value);
	    }
	    if (value instanceof Short) {
	      return ((Short)value).doubleValue();
	    }
	    if (value instanceof Integer) {
	      return ((Integer)value).doubleValue();
	    }
	    if (value instanceof Byte) {
	      return ((Byte)value).doubleValue();
	    }
	    if (value instanceof Long) {
	      return ((Long)value).doubleValue();
	    }
	    if (value instanceof Float) {
	      return ((Float)value).doubleValue();
	    }
	    if (value instanceof Number) {
	      return ((Number)value).doubleValue();
	    }
	    throw new IllegalArgumentException("");
	  }
}
