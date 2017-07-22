package jenkins.metrics.impl;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Metric;
import jenkins.metrics.api.Metrics;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.util.Map;

/**
 * @author Sam Van Oort
 */
public class LinuxDiskStatsTest {
    @Test
    public void testDiskStatParsing() throws Exception {
        LinuxDiskStatsProviderImpl metrics = new LinuxDiskStatsProviderImpl();
        File file = new File(LinuxDiskStatsTest.class.getClassLoader().getResource("org/jenkinsci/plugins/linuxmetrics/diskstats").getFile());
        metrics.updateMetrics(file);

        Map<String, Metric> registeredMetrics = Metrics.metricRegistry().getMetrics();
        String[] devices = {"nbd15", "vda", "vda1"};

        // All totals should be set
        for (String metric : metrics.METRIC_NAMES) {
            Assert.assertTrue("No total registered for metric: "+metric, registeredMetrics.containsKey("total."+metric));
        }

        // Check we registered the metrics
        for (String device  : devices) {
            for (String metric : metrics.METRIC_NAMES) {
                Assert.assertTrue("No total registered for metric: "+metric+" and device "+device,
                        registeredMetrics.containsKey(device+'.'+metric));
            }
        }

        Assert.assertEquals(4930, ((Counter)(registeredMetrics.get("vda1.successfulReads"))).getCount());
        Assert.assertEquals(31337472, ((Counter)(registeredMetrics.get("vda.bytesRead"))).getCount());
    }
}
