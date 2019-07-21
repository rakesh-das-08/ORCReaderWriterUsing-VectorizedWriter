import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.util.Locale;

class RuntimeMemoryUtil {
    private static final long MEGABYTE_FACTOR = 1024L * 1024L;
    private static final DecimalFormat ROUNDED_DOUBLE_DECIMALFORMAT;
    private static final String MB = "MB";
    private static Runtime rt = Runtime.getRuntime();

    static {
        DecimalFormatSymbols otherSymbols = new DecimalFormatSymbols(Locale.ENGLISH);
        otherSymbols.setDecimalSeparator('.');
        otherSymbols.setGroupingSeparator(',');
        ROUNDED_DOUBLE_DECIMALFORMAT = new DecimalFormat("####0.00", otherSymbols);
        ROUNDED_DOUBLE_DECIMALFORMAT.setGroupingUsed(false);
    }

     static long bytesToMiB(long bytes){
        return bytes/MEGABYTE_FACTOR;
    }


     static String getTotalMemoryInMB() {
        double totalMiB = bytesToMiB(rt.totalMemory());
        return String.format("%s %s", ROUNDED_DOUBLE_DECIMALFORMAT.format(totalMiB), MB);
    }

     static String getMaxMemoryInMB() {
        double maxMiB = bytesToMiB(rt.maxMemory());
        return String.format("%s %s", ROUNDED_DOUBLE_DECIMALFORMAT.format(maxMiB), MB);
    }

     static String getFreeMemoryInMB() {
        double freeMiB = bytesToMiB(rt.freeMemory());
        return String.format("%s %s", ROUNDED_DOUBLE_DECIMALFORMAT.format(freeMiB), MB);
    }

     static String getUsedMemoryInMB() {
        double usedMiB = bytesToMiB(rt.maxMemory()-rt.freeMemory());
        return String.format("%s %s", ROUNDED_DOUBLE_DECIMALFORMAT.format(usedMiB), MB);
    }


     static double getPercentageUsed() {
        return ((double) (rt.maxMemory()-rt.freeMemory()) / rt.maxMemory()) * 100;
    }

     static String getPercentageUsedFormatted() {
        double usedPercentage = getPercentageUsed();
        return ROUNDED_DOUBLE_DECIMALFORMAT.format(usedPercentage) + "%";
    }
}
