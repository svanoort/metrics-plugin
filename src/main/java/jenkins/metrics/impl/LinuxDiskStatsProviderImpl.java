package jenkins.metrics.impl;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.MetricSet;
import com.google.common.base.Function;
import com.google.common.base.Splitter;
import edu.umd.cs.findbugs.annotations.NonNull;
import hudson.Extension;
import hudson.Platform;
import hudson.model.PeriodicWork;
import jenkins.metrics.api.MetricProvider;
import jenkins.metrics.api.Metrics;
import jenkins.model.Jenkins;
import org.apache.commons.io.FileUtils;

import javax.annotation.Nonnull;
import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Pattern;

/**
 *  For linux disk stats - parse /proc/diskstats to get IO stats
 *  See: https://www.kernel.org/doc/Documentation/ABI/testing/procfs-diskstats
 * @author Sam Van Oort
 */
@Extension
 public class LinuxDiskStatsProviderImpl extends MetricProvider {
    static final String METRIC_PREFIX = "linuxstats.diskstats";

    static File PROC_FILE = new File("/proc/diskstats");

    static final Splitter SPLITTER = Splitter.on(Pattern.compile("\\s+")).omitEmptyStrings();

    private volatile long NEXT_REFRESH = Long.MIN_VALUE;

    static final Logger myLogger = Logger.getLogger(LinuxDiskStatsProviderImpl.class.getName());

    static long REFRESH_INTERVAL_MILLIS = 15000L;

    // See https://www.kernel.org/doc/Documentation/ABI/testing/procfs-diskstats for order, and we skip the first 3
    static final String[] METRIC_NAMES = {
            "successfulReads",
            "mergedReads",
            "bytesRead",  // Sectors written originally, but we convert to bytes
            "readTimeMillis",
            "successfulWrites",
            "mergedWrites",
            "bytesWritten",  // Sectors written originally, but we convert to bytes
            "writeTimeMillis",
            "inProgressIOPS",
            "totalIOMillis",
            "weightedIOMillis"
    };

    HashMap<String, ExtendedDeviceStatsRow> currentDeviceStats = new HashMap<String, ExtendedDeviceStatsRow>();

    /** Read from a single disk device, with fields in the same order as given */
    static class DeviceStatsRow {
        String deviceName;

        /** Fields from {@link #METRIC_NAMES} */
        final long[] stats = new long[METRIC_NAMES.length];

        long lastUpdateMillis;

        public long getSuccessfulReads() {return stats[0];}
        public long getMergedReads() {return stats[1];}
        public long getBytesRead() {return stats[2];}
        public long getReadTimeMillis() {return stats[3];}
        public long getSuccessfulWrites() {return stats[4];}
        public long getMergedWrites() {return stats[5];}
        public long getBytesWritten() {return stats[6];}
        public long getWriteTimeMillis() {return stats[7];}

        void add(@Nonnull  DeviceStatsRow adder) {
            for(int i=0; i<stats.length; i++) {
                stats[i] += adder.stats[i];
            }
        }

        /** Resets stats and timing using new data */
        void updateWithNewStats(@Nonnull DeviceStatsRow source) {
            this.lastUpdateMillis = source.lastUpdateMillis;
            System.arraycopy(source.stats, 0, this.stats, 0, METRIC_NAMES.length);
        }

        void generateAbsentMetrics(@Nonnull  MetricRegistry registry) {
            Map<String,Metric> metricMap = registry.getMetrics();
            for (int i=0; i<METRIC_NAMES.length; i++) {
                String metricName = MetricRegistry.name(METRIC_PREFIX, deviceName, METRIC_NAMES[i]);
                Metric metr = metricMap.get(metricName);

                if (metr == null) {
                    registry.register(metricName, new DiskStatGauge(this, i));
                }
            }
        }
    }

    static class ExtendedDeviceStatsRow extends DeviceStatsRow {
        DeviceStatsRow prev;

        public ExtendedDeviceStatsRow(@Nonnull DeviceStatsRow source) {
            this.deviceName = source.deviceName;
            super.updateWithNewStats(source);
        }

        public long getElapsedTimeMillis()  { return this.lastUpdateMillis-prev.lastUpdateMillis; }
        public long getNewReads()  { return this.getSuccessfulReads()-prev.getSuccessfulReads(); }
        public long getNewWrites()  { return this.getSuccessfulWrites()-prev.getSuccessfulWrites(); }
        public long getNewReadBytes()  { return this.getBytesRead()-prev.getBytesRead(); }
        public long getNewWriteBytes()  { return this.getBytesWritten()-prev.getBytesWritten(); }

        public double getIOPS() {
            return (double)(getNewReads()+getNewWrites()) / (getElapsedTimeMillis()/1000.0);
        }

        static final Function<ExtendedDeviceStatsRow, Double> IOPS_FUNCTION = new Function<ExtendedDeviceStatsRow, Double>() {
            @Override
            public Double apply(@NonNull ExtendedDeviceStatsRow extendedDeviceStatsRow) {
                return extendedDeviceStatsRow.getIOPS();
            }
        };

        /** B/s */
        public double getReadThroughput() {
            return (double)(getNewReadBytes())/(getElapsedTimeMillis()/1000.0);
        }

        static final Function<ExtendedDeviceStatsRow, Double> READ_THROUGHPUT_FUNCTION = new Function<ExtendedDeviceStatsRow, Double>() {
            @Override
            public Double apply(@NonNull ExtendedDeviceStatsRow extendedDeviceStatsRow) {
                return extendedDeviceStatsRow.getReadThroughput();
            }
        };

        /** B/s */
        public double getWriteThroughput() {
            return (double)(getNewWriteBytes())/(getElapsedTimeMillis()/1000.0);
        }

        static final Function<ExtendedDeviceStatsRow, Double> WRITE_THROUGHPUT_FUNCTION = new Function<ExtendedDeviceStatsRow, Double>() {
            @Override
            public Double apply(@NonNull ExtendedDeviceStatsRow extendedDeviceStatsRow) {
                return extendedDeviceStatsRow.getWriteThroughput();
            }
        };

        public double getMergedReadFraction() {
            double value = (double)(this.getMergedReads()-prev.getMergedReads())/(double)getNewReads();
            return (Double.isNaN(value)) ? 0.0 : value;
        }

        static final Function<ExtendedDeviceStatsRow, Double> MERGED_READ_FRACTION_FUNCTION = new Function<ExtendedDeviceStatsRow, Double>() {
            @Override
            public Double apply(@NonNull ExtendedDeviceStatsRow extendedDeviceStatsRow) {
                return extendedDeviceStatsRow.getMergedReadFraction();
            }
        };

        public double getMergedWriteFraction() {
            double value = (this.getMergedWrites()-prev.getMergedWrites())/(double)getNewWrites();
            return (Double.isNaN(value)) ? 0.0 : value;
        }

        static final Function<ExtendedDeviceStatsRow, Double> MERGED_WRITE_FRACTION_FUNCTION = new Function<ExtendedDeviceStatsRow, Double>() {
            @Override
            public Double apply(@NonNull ExtendedDeviceStatsRow extendedDeviceStatsRow) {
                return extendedDeviceStatsRow.getMergedWriteFraction();
            }
        };

        public double getIoReadTimeFraction() {
            return (double)(this.getReadTimeMillis()-prev.getReadTimeMillis())/(double)getElapsedTimeMillis();
        }

        static final Function<ExtendedDeviceStatsRow, Double> IO_READ_TIME_FRACTION_FUNCTION = new Function<ExtendedDeviceStatsRow, Double>() {
            @Override
            public Double apply(@NonNull ExtendedDeviceStatsRow extendedDeviceStatsRow) {
                return extendedDeviceStatsRow.getIoReadTimeFraction();
            }
        };

        public double getIoWriteTimeFraction() {
            return (double)(this.getWriteTimeMillis()-prev.getWriteTimeMillis())/(double)getElapsedTimeMillis();
        }

        static final Function<ExtendedDeviceStatsRow, Double> IO_WRITE_TIME_FRACTION_FUNCTION = new Function<ExtendedDeviceStatsRow, Double>() {
            @Override
            public Double apply(@Nonnull ExtendedDeviceStatsRow extendedDeviceStatsRow) {
                return extendedDeviceStatsRow.getIoWriteTimeFraction();
            }
        };

        /** Computes deltas */
        @Override
        void updateWithNewStats(@Nonnull DeviceStatsRow source) {
            if (prev != null) {
                prev.updateWithNewStats(this);
            } else {
                prev = new DeviceStatsRow();
                prev.deviceName = this.deviceName;
                prev.updateWithNewStats(this);
            }
            super.updateWithNewStats(source);
        }

        static class ExtendedDiskStatsTxformGauge<T> implements Gauge<T> {
            ExtendedDeviceStatsRow row;
            Function<ExtendedDeviceStatsRow,T> transform;

            @Override
            public T getValue() {
                return transform.apply(row);
            }

            // Fluent-style configurators
            ExtendedDiskStatsTxformGauge setRow(ExtendedDeviceStatsRow myRow) {
                this.row = myRow;
                return this;
            }

            ExtendedDiskStatsTxformGauge setFunction(Function<ExtendedDeviceStatsRow,T> funct) {
                this.transform = funct;
                return this;
            }
        }

        /** Generates the derived metrics and registers to the TOP-LEVEL registry */
        void generateExtendedMetrics(@Nonnull MetricRegistry registry) {

            registry.register(MetricRegistry.name(METRIC_PREFIX, deviceName, "iops"), new ExtendedDiskStatsTxformGauge<Double>().setRow(this).setFunction(IOPS_FUNCTION));
            registry.register(MetricRegistry.name(METRIC_PREFIX, deviceName, "readThroughput"), new ExtendedDiskStatsTxformGauge<Double>().setRow(this).setFunction(READ_THROUGHPUT_FUNCTION));
            registry.register(MetricRegistry.name(METRIC_PREFIX,  deviceName, "writeThroughput"), new ExtendedDiskStatsTxformGauge<Double>().setRow(this).setFunction(WRITE_THROUGHPUT_FUNCTION));
            registry.register(MetricRegistry.name(METRIC_PREFIX, deviceName, "mergedReadFraction"), new ExtendedDiskStatsTxformGauge<Double>().setRow(this).setFunction(MERGED_READ_FRACTION_FUNCTION));
            registry.register(MetricRegistry.name(METRIC_PREFIX, deviceName, "mergedWriteFraction"), new ExtendedDiskStatsTxformGauge<Double>().setRow(this).setFunction(MERGED_WRITE_FRACTION_FUNCTION));
            registry.register(MetricRegistry.name(METRIC_PREFIX, deviceName, "ioReadTimeFraction"), new ExtendedDiskStatsTxformGauge<Double>().setRow(this).setFunction(IO_READ_TIME_FRACTION_FUNCTION));
            registry.register(MetricRegistry.name(METRIC_PREFIX, deviceName, "ioWriteTimeFraction"), new ExtendedDiskStatsTxformGauge<Double>().setRow(this).setFunction(IO_WRITE_TIME_FRACTION_FUNCTION));
        }
    }

    /** Take the string corresponding to a row of disk stats and parse it into fields, then store to the given statsrow */
    DeviceStatsRow readDeviceStatsRow(@Nonnull String line, @Nonnull DeviceStatsRow statsRow) {
        Iterator<String> tokens = SPLITTER.split(line).iterator();
        tokens.next(); tokens.next();  // Serial numbers are irrelevant

        statsRow.deviceName = tokens.next();
        for (int i=0; i<METRIC_NAMES.length; i++ ) {
            long val = Long.parseLong(tokens.next());
            if (i == 2 || i == 6) { // Sector counts, we multiply by 512 to report bytes instead
                val *= 512;
            }
            statsRow.stats[i] = val;
        }

        return statsRow;
    }

    /** Parse device stats from the file line with {@link #readDeviceStatsRow(String, DeviceStatsRow)} and store to a new DeviceStatsRow */
    DeviceStatsRow readDeviceStatsRow(String line) {
        return readDeviceStatsRow(line, new DeviceStatsRow());
    }

    /** Reads stored diskstats */
    static class DiskStatGauge implements Gauge<Long> {
        final int statIndex;
        final DeviceStatsRow source;

        DiskStatGauge(DeviceStatsRow row, int index) {
            this.statIndex = index;
            source = row;
        }

        @Override
        public Long getValue() {
            return source.stats[statIndex];
        }
    }

    /** Triggers a local update if possible */
    void update() {
        if (Platform.current() == Platform.UNIX && !Platform.isDarwin() && PROC_FILE.exists()) {
            try {
                updateMetrics(PROC_FILE);
            } catch (IOException ioe) {
                myLogger.log(Level.WARNING, "Error gathering linux disk metrics", ioe);
            }
        }
    }

    /** Reads diskstats from the given file and creates or updates metrics
     *  This includes updating top-level registered metrics so we catch newly added devices
     */
    void updateMetrics(@Nonnull File metricsFile) throws IOException {
        List<String> lines = FileUtils.readLines(metricsFile);

        MetricRegistry topRegistry = Metrics.metricRegistry();

        DeviceStatsRow totals = new DeviceStatsRow();
        totals.deviceName = "total";

        // Read off each device, and get the value, then update counters accordingly
        for (String line : lines) {
            long timeStamp = System.currentTimeMillis();
            DeviceStatsRow stats = readDeviceStatsRow(line);
            stats.lastUpdateMillis = timeStamp;
            totals.add(stats);

            ExtendedDeviceStatsRow prevStats = currentDeviceStats.get(stats.deviceName);
            if (prevStats == null) {
                stats.generateAbsentMetrics(topRegistry);
                ExtendedDeviceStatsRow extd = new ExtendedDeviceStatsRow(stats);
                currentDeviceStats.put(stats.deviceName, extd);
            } else {
                if (prevStats.prev == null) {
                    prevStats.updateWithNewStats(stats);
                    prevStats.generateExtendedMetrics(topRegistry);
                } else {
                    prevStats.updateWithNewStats(stats);
                }

            }
        }
        DeviceStatsRow prevTotals = currentDeviceStats.get("total");
        if (prevTotals == null) {
            currentDeviceStats.put("total", new ExtendedDeviceStatsRow(totals));
            totals.generateAbsentMetrics(topRegistry);
        } else {
            prevTotals.updateWithNewStats(totals);
        }
    }

    @Extension
    public static class PeriodicWorkImpl extends PeriodicWork {
        @Override
        public long getRecurrencePeriod() {
            return TimeUnit.SECONDS
                    .toMillis(5); // the meters expect to be ticked every 5 seconds to give a valid m1, m5 and m15
        }

        @Override
        protected synchronized void doRun() throws Exception {
            final LinuxDiskStatsProviderImpl instance = instance();
            if (instance != null) {
                instance.update();
            }
        }
    }

    public static LinuxDiskStatsProviderImpl instance() {
        Jenkins jenkins = Jenkins.getInstance();
        return jenkins == null ? null : jenkins.getExtensionList(MetricProvider.class).get(LinuxDiskStatsProviderImpl.class);
    }

    @NonNull
    @Override
    public synchronized MetricSet getMetricSet() {
        // Total hack, but this allows update() to guarantee metric registration
        update();
        return new MetricRegistry();
    }
}
