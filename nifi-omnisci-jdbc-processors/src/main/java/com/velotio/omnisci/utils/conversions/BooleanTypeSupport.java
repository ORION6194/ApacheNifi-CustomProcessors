package com.velotio.omnisci.utils.conversions;
import java.math.BigDecimal;
public class BooleanTypeSupport extends TypeSupport<Boolean> {
	@Override
	  public Boolean convert(Object value) {
	    if (value instanceof Boolean) {
	      return (Boolean) value;
	    }
	    if (value instanceof String) {
	      return Boolean.valueOf((String) value);
	    }
	    if (value instanceof Integer) {
	      return ((Integer)value) != 0;
	    }
	    if (value instanceof Long) {
	      return ((Long)value) != 0;
	    }
	    if (value instanceof Short) {
	      return ((Short)value) != 0;
	    }
	    if (value instanceof Byte) {
	      return ((Byte)value) != 0;
	    }
	    if (value instanceof Float) {
	      return ((Float)value) != 0;
	    }
	    if (value instanceof Double) {
	      return ((Double)value) != 0;
	    }
	    if (value instanceof BigDecimal) {
	      return ! value.equals(BigDecimal.ZERO);
	    }
	    throw new IllegalArgumentException("");
	  }
}
