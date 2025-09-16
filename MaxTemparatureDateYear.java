import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MaxTemperatureDateYear {

    // Mapper: Extract year as key, and (date,temp) as value
    public static class TempMapper
            extends Mapper<Object, Text, Text, Text> {

        private Text year = new Text();
        private Text dateTemp = new Text();

        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {

            // Example line: 2020-06-20 45
            String line = value.toString().trim();
            if (line.isEmpty()) return;

            String[] parts = line.split("\\s+");
            if (parts.length < 2) return;

            String date = parts[0];      // full date YYYY-MM-DD
            String yearStr = date.substring(0, 4);
            String temp = parts[1];

            year.set(yearStr);
            dateTemp.set(date + "," + temp); // date,temp
            context.write(year, dateTemp);
        }
    }

    // Reducer: Find max temperature for each year
    public static class TempReducer
            extends Reducer<Text, Text, Text, Text> {

        private Text result = new Text();

        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            int maxTemp = Integer.MIN_VALUE;
            String maxDate = "";

            for (Text val : values) {
                String[] parts = val.toString().split(",");
                String date = parts[0];
                int temp = Integer.parseInt(parts[1]);

                if (temp > maxTemp) {
                    maxTemp = temp;
                    maxDate = date;
                }
            }

            result.set(maxTemp + "\t" + maxDate);
            context.write(key, result);
        }
    }

    // Driver
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Max Temperature Per Year");
        job.setJarByClass(MaxTemperatureDateYear.class);

        job.setMapperClass(TempMapper.class);
        job.setReducerClass(TempReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
