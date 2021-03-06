package Question1;

import java.nio.charset.Charset;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.interceptor.*;

public class TimestampInterceptor implements Interceptor {

    private String fileName;

    public TimestampInterceptor() {

    }

    public void initialize() {
        // no-op
    }

    /**
     * Modifies events in-place.
     */

    public Event intercept(Event event) {

        try {
            String eventBody = new String(event.getBody(), "UTF-8");
            
            Date date = new Date();
            String parseTime = "Parse time: ";

            // This is pretty close to full ISO8601 format, so this 
            // should be great for us
            SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS XXX");
            parseTime += format.format(date) + ", ";

            // Use String builder to write the new Event.
            StringBuilder builder = new StringBuilder();
            builder.append(parseTime);
            builder.append(eventBody);
            Event e = EventBuilder.withBody(builder.toString(),
                    Charset.forName("UTF-8"));
            return e;
        } catch (Exception exp) {
            return null;
        }
    }

    /**
     * Delegates to {@link #intercept(Event)} in a loop.
     * 
     * @param events
     * @return
     */

    public List<Event> intercept(List<Event> events) {
        List<Event> eventList = new ArrayList<Event>();
        for (Event event : events) {
            Event outEvent = intercept(event);
            if (outEvent != null) {
                eventList.add(outEvent);
            }
        }
        return eventList;
    }

    public void close() {
        // no-op
    }

    public static class Builder implements Interceptor.Builder {

        public Interceptor build() {
            return new TimestampInterceptor();
        }

        public void configure(Context arg0) {
            // TODO Auto-generated method stub

        }

    }

}
