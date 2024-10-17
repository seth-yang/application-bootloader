package org.dreamwork.config;

import org.dreamwork.util.StringUtil;

import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class EnhancedConfiguration extends PropertyConfiguration {
    private static final Pattern P = Pattern.compile ("\\$\\{(.*?)\\}");

    public EnhancedConfiguration (Properties props) {
        super (props);
    }

    @Override
    public String getString (String key, Object... params) {
        String value = super.getString (key, params);
        if (value != null && value.contains ("${")) {
            value = replace (value);
        }
        return value;
    }

    @Override
    public String getString (String key, KeyValuePair<?>... params) {
        String value = super.getString (key, params);
        if (value != null && value.contains ("${")) {
            value = replace (value);
        }
        return value;
    }

    private String replace (String value) {
        Matcher m = P.matcher (value);
        Object[] args = new Object[0];
        StringBuffer buffer = new StringBuffer ();
        while (m.find ()) {
            String part = m.group (1);
            String replacement = getString (part, args);
            if (!StringUtil.isEmpty (replacement)) {
                m.appendReplacement (buffer, replacement);
            }
        }
        m.appendTail (buffer);
        return buffer.toString ();
    }
}