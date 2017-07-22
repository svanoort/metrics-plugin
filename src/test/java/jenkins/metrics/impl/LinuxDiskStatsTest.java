package jenkins.metrics.impl;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricRegistry;
import jenkins.metrics.api.Metrics;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.io.File;
import java.util.Map;

/**
 * @author Sam Van Oort
 */
public class LinuxDiskStatsTest {
    @Test
    @Ignore // Broken by a refactoring
    public void testDiskStatParsing() throws Exception {
        LinuxDiskStatsProviderImpl metrics = new LinuxDiskStatsProviderImpl();
        File file = new File(LinuxDiskStatsTest.class.getClassLoader().getResource("jenkins/metrics/impl/diskstats").getFile());

        MetricRegistry testRegistry = new MetricRegistry();
        metrics.updateMetrics(file, testRegistry);
        Map<String, Metric> registryMap = testRegistry.getMetrics();

        String[] devices = {"nbd15", "vda", "vda1"};

        // All totals should be set
        for (String metric : metrics.METRIC_NAMES) {
            Assert.assertTrue("No total registered for metric: "+metric, registryMap.containsKey("total."+metric));
        }

        // Check we registered the metrics
        for (String device  : devices) {
            for (String metric : metrics.METRIC_NAMES) {
                Assert.assertTrue("No total registered for metric: "+metric+" and device "+device,
                        registryMap.containsKey(device+'.'+metric));
            }
        }

        Assert.assertEquals(4930, ((Counter)(registryMap.get("vda1.successfulReads"))).getCount());
        Assert.assertEquals(31337472, ((Counter)(registryMap.get("vda.bytesRead"))).getCount());
    }
}
