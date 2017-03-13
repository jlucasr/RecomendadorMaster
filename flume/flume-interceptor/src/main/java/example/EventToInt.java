package example;

import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;


public class EventToInt implements Interceptor{

	public void close() {
		// Do nothing
	}

	public void initialize() {
		
	}

	//metodo modificado
	public Event intercept(Event event) {
		// Create a string representation of the line coming in to Flume
		String lineEvent = new String(event.getBody());

		StringTokenizer tokenizer = new StringTokenizer(lineEvent,"\t");
		String cadUser = (String)tokenizer.nextElement();
		String numberUser = cadUser.substring(5,cadUser.length());
		
		// reemplazar en la cadena principal
		String changedEvent = lineEvent.replaceAll(cadUser,numberUser);

		// Write the bytes to the body
		event.setBody(changedEvent.getBytes());

		return event;
	}

	public List<Event> intercept(List<Event> events) {
		ArrayList<Event> eventList = new ArrayList<Event>();

		for (Event event : events) {
			eventList.add(intercept(event));
		}

		return eventList;
	}

	public static class Builder implements Interceptor.Builder {

		public void configure(Context context) {
			// Do nothing
		}

		public Interceptor build() {
			return new EventToInt();
		}

	}
}
