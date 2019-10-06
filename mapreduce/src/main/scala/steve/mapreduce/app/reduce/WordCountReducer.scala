package steve.mapreduce.app.reduce

import java.lang
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapreduce.Reducer

class WordCountReducer extends Reducer[Text, LongWritable, Text, LongWritable]{
  override def reduce(key: Text, values: lang.Iterable[LongWritable], context: Reducer[Text, LongWritable, Text, LongWritable]#Context): Unit = {
    var sum: Long = 0L
    while (values.iterator().hasNext) sum += values.iterator().next().get()
    context.write(key, new LongWritable(sum))
  }
}
