package com.microsoft.azure.eventhubs.kafka.connect.sink;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.storage.ConverterConfig;

import java.time.format.DateTimeFormatter;
import java.util.Map;

public class OutputJsonFormatterConfig extends AbstractConfig {

        public static final String FORMATS_DATE_TIME_CONFIG = "json.datetime.pattern";
        public static final String FORMATS_DATE_TIME_DEFAULT = "";
        private static final String FORMATS_DATE_TIME_DOC = "Date time format using DateTimeFormatter";
        private static final String FORMATS_DATE_TIME_DISPLAY = "Datetime format";

        public static final String FORMATS_DATE_CONFIG = "json.date.pattern";
        public static final String FORMATS_DATE_DEFAULT = "";
        private static final String FORMATS_DATE_DOC = "Date formatting string using DateTimeFormatter";
        private static final String FORMATS_DATE_DISPLAY = "Date format";

        public static final String NULL_CONFIG = "json.skip.null";
        public static final boolean NULL_DEFAULT = true;
        private static final String NULL_DOC = "Skip null fields while converting to JSON";
        private static final String NULL_DISPLAY  = "Skip JSON nulls";


        private final static ConfigDef CONFIG;

        static {
            String group = "Schemas";
            int orderInGroup = 0;
            CONFIG = ConverterConfig.newConfigDef();
            CONFIG.define(FORMATS_DATE_TIME_CONFIG, ConfigDef.Type.STRING, FORMATS_DATE_TIME_DEFAULT, ConfigDef.Importance.LOW, FORMATS_DATE_TIME_DOC, group,
                    orderInGroup++, ConfigDef.Width.MEDIUM, FORMATS_DATE_TIME_DISPLAY);
            CONFIG.define(FORMATS_DATE_CONFIG, ConfigDef.Type.STRING, FORMATS_DATE_DEFAULT, ConfigDef.Importance.LOW, FORMATS_DATE_DOC, group,
                    orderInGroup++, ConfigDef.Width.MEDIUM, FORMATS_DATE_DISPLAY);
            CONFIG.define(NULL_CONFIG, ConfigDef.Type.BOOLEAN, NULL_DEFAULT, ConfigDef.Importance.LOW, NULL_DOC, group,
                    orderInGroup++, ConfigDef.Width.MEDIUM, NULL_DISPLAY);
        }

        public static ConfigDef configDef() {
            return CONFIG;
        }

        public OutputJsonFormatterConfig(Map<String, ?> props) {
            super(CONFIG, props);
        }

        /**
         * Return format for DateTime
         *
         * @return DateTimeFormatter instance
         */
        public DateTimeFormatter dateTimeFormat() {
            if (getString(FORMATS_DATE_TIME_CONFIG).isEmpty()) {
                return DateTimeFormatter.ISO_LOCAL_DATE_TIME;
            } else {
                return DateTimeFormatter.ofPattern(getString(FORMATS_DATE_TIME_CONFIG));
            }
        }

        /**
         * Return format for Date
         *
         * @return DateTimeFormatter instance
         */

        public DateTimeFormatter dateFormat() {
            if (getString(FORMATS_DATE_CONFIG).isEmpty()) {
                return DateTimeFormatter.ISO_LOCAL_DATE;
            } else {
                return DateTimeFormatter.ofPattern(getString(FORMATS_DATE_CONFIG));
            }
        }

    /**
     *  Return configuration for JSON nulls behavior. If enabled nulls values are not printed in JsonSerializer output.
     * @return
     */
    public boolean enableSkipNulls() {
            return getBoolean(NULL_CONFIG);
        }

}
