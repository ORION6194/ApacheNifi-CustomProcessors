package com.velotio.omnisci.utils.conversions;
import java.text.ParseException;
import java.util.Date;
public class DateTypeSupport extends TypeSupport<Date> {
	@Override
	  public Date convert(Object value) {
	    if (value instanceof Date) {
	      return (Date) value;
	    }
	    if (value instanceof String) {
	      try {
	        return Utils.parse((String) value);
	      } catch (ParseException ex) {
	        throw new IllegalArgumentException("");
	      }
	    }
	    if (value instanceof Long) {
	      return new Date((long) value);
	    }
	    if (value instanceof Integer) {
	      return new Date((int) value);
	    }
	    throw new IllegalArgumentException("");
	  }

	  @Override
	  public Object create(Object value) {
	    return clone(value);
	  }

	  @Override
	  public Object get(Object value) {
	    return clone(value);
	  }

	  @Override
	  public Object clone(Object value) {
	    // Various sources can return subclass of java.sql.Date (for example JDBC) which can change implementation of
	    // toString() and other methods that we're using for serialization. To avoid any such trobules, we're creating
	    // our own instance of Date from epoch.
	    return new Date(((Date) value).getTime());
	  }

}
