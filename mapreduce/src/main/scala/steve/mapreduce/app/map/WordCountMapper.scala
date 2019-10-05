package steve.mapreduce.app.map

import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapreduce.Mapper

class WordCountMapper extends Mapper[LongWritable, Text, Text, LongWritable]{
  override def map(key: LongWritable, value: Text, context: Mapper[LongWritable, Text, Text, LongWritable]#Context): Unit = {
     value.toString
       .split("\\s")
       .map((_, 1))
       .groupBy(_._1)
       .mapValues(v => v.map(_._2).sum)
       .foreach(m => context.write(new Text(m._1), new LongWritable(m._2)))
  }
}
