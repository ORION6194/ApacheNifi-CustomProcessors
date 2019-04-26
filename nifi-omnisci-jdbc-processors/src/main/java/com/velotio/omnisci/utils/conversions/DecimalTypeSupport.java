package com.velotio.omnisci.utils.conversions;
import java.math.BigDecimal;
public class DecimalTypeSupport extends TypeSupport<BigDecimal> {
	@Override
	  public BigDecimal convert(Object value) {
	    if (value instanceof BigDecimal) {
	      return (BigDecimal) value;
	    }

	    if (value instanceof String) {
	      return new BigDecimal((String) value);
	    }

	    if (value instanceof Number) {
	      //http://stackoverflow.com/questions/16216248/convert-java-number-to-bigdecimal-best-way
	      return new BigDecimal(value.toString());
	    }

	    throw new IllegalArgumentException("");
	  }
}
