package com.dsp.ass2.steps;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.junit.Before;
import org.junit.Test;

public class AggregationStepTest {

    private AggregationStep.AggregationMapper mapper;
    private Mapper.Context context;
    private Configuration conf;

    @Before
    public void setUp() {
        mapper = new AggregationStep.AggregationMapper();
        context = mock(Mapper.Context.class);
        conf = new Configuration();

        // Mock Context.getConfiguration()
        when(context.getConfiguration()).thenReturn(conf);

        // We can't easily mock the StopWords inner loading because it instantiates `new
        // StopWords` inside setup().
        // However, we can use a real StopWords instance if we provide a dummy file, OR
        // we just let it fail gracefully if file missing?
        // StopWords catches exception and just has empty set.
        // Let's rely on empty set behavior for now, or set a path to a non-existent
        // file which results in no stop words.
        conf.set("stopwords.path", "dummy/path");
    }

    @Test
    public void testMap_ValidInput() throws Exception {
        // Init
        // We need to call setup() manually because we are unit testing the mapper class
        // directly
        // Reflection or just call it if visible? It is protected.
        // We can subclass it in test or just assume logic doesn't depend heavily on
        // setup if we mock StopWords...
        // Wait, mapper.setup(context) initializes StopWords.

        // Trick: Subclass to expose setup or use reflection.
        // Or just test map() logic implying StopWords is empty (default if file not
        // found).

        TestMapper testMapper = new TestMapper();
        testMapper.setup(context); // Initialize StopWords (empty)

        // Input: "high school 1990 10"
        Text value = new Text("high school\t1990\t10");
        LongWritable key = new LongWritable(0);

        testMapper.map(key, value, context);

        // Expected Output: (1990 \t high \t school, 10)
        verify(context).write(eq(new Text("1990\thigh\tschool")), eq(new LongWritable(10)));
    }

    @Test
    public void testMap_Malformed() throws Exception {
        TestMapper testMapper = new TestMapper();
        testMapper.setup(context);

        Text value = new Text("bad line");
        testMapper.map(new LongWritable(0), value, context);

        verify(context, never()).write(any(), any());
    }

    // Subclass to access protected setup
    public static class TestMapper extends AggregationStep.AggregationMapper {
        @Override
        public void setup(Context context) {
            super.setup(context);
        }
    }
}
