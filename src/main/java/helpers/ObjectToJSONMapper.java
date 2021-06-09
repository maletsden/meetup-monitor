package helpers;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import scala.Tuple2;

public final class ObjectToJSONMapper {
    public static <K, V> String tuple2ToJSON(Tuple2<K, V> pair) {
        ObjectWriter ow = new ObjectMapper().writer().withDefaultPrettyPrinter();

        String json = "";
        try {
            json = ow.writeValueAsString(pair._2);
        } catch (JsonProcessingException e) {
            System.out.println("Top10VideosAnalyzer.videoDescriptionToJSONMapper:: " +
                    "ERROR: failed parse VideoDescription to JSON.");
            e.printStackTrace();
        }

        return json;

    }

    public static <T> String objToJSON(T obj) {
        ObjectWriter ow = new ObjectMapper().writer().withDefaultPrettyPrinter();

        String json = "";
        try {
            json = ow.writeValueAsString(obj);
        } catch (JsonProcessingException e) {
            System.out.println("Top10VideosAnalyzer.videoDescriptionToJSONMapper:: " +
                    "ERROR: failed parse VideoDescription to JSON.");
            e.printStackTrace();
        }

        return json;
    }
}
