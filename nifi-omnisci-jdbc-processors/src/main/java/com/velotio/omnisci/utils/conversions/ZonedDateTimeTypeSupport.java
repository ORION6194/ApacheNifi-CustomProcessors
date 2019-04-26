package com.velotio.omnisci.utils.conversions;

import java.time.ZonedDateTime;
import java.time.format.DateTimeParseException;

	public class ZonedDateTimeTypeSupport extends TypeSupport<ZonedDateTime> {
		  @Override
		  public ZonedDateTime convert(Object value) {
		    if (value instanceof ZonedDateTime) {
		      return (ZonedDateTime) value;
		    }

		    // We don't use ZonedDateTime.toString() to convert to String, since it requires offset and ID to be the same,
		    // We convert to String using DateTimeFormatter.ISO_ZONED_DATE_TIME (which is default for this method, but passed
		    // in explicitly for clarity)
		    try {
		      Utils.checkArgument(
		          value instanceof String,""
		      );
		      return Utils.parseZoned((String) value);
		    } catch (DateTimeParseException ex) {
		      throw new IllegalArgumentException("");
		    }
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
		    // ZonedDateTime instances are immutable
		    return value;
		  }
}
