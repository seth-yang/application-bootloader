package org.dreamwork.app.bootloader;

import com.google.gson.Gson;
import org.apache.log4j.PropertyConfigurator;
import org.dreamwork.cli.Argument;
import org.dreamwork.cli.ArgumentParser;
import org.dreamwork.config.IConfiguration;
import org.dreamwork.config.PropertyConfiguration;
import org.dreamwork.util.FileInfo;
import org.dreamwork.util.IOUtil;
import org.dreamwork.util.StringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;

public class ApplicationBootloader {
    private static Map<String, IConfiguration> context = new HashMap<> ();

    public static IConfiguration getRootConfiguration () {
        return context.get ("root");
    }

    public static IConfiguration getConfiguration (String name) {
        return context.get (name);
    }

    public static void run (Class<?> type, String[] args) {
        ClassLoader loader = ApplicationBootloader.class.getClassLoader ();

        Map<String, Argument> map = new HashMap<> ();
        Gson g = new Gson ();

        load (loader, g, map, "application-bootloader.json");
        if (type != null) {
            if (type.isAnnotationPresent (IBootable.class)) {
                IBootable ib = type.getAnnotation (IBootable.class);
                String argDef = ib.argumentDef ();
                if (StringUtil.isEmpty (argDef)) {
                    argDef = StringUtil.camelDecode (type.getName (), '-') + ".json";
                }

                load (loader, g, map, argDef);
            }
        }

        ArgumentParser parser = null;
        if (!map.isEmpty ()) {
            parser = new ArgumentParser (new ArrayList<> (map.values ()));
        }

        if (parser == null) {
            System.err.println ("can't initial command line parser");
            System.exit (-1);
            return;
        }

        parser.parse (args);
        try {
            initLogger (loader, parser);
        } catch (IOException ex) {
            throw new RuntimeException (ex);
        }

        Logger logger = LoggerFactory.getLogger (ApplicationBootloader.class);
        try {
            Properties props = parseConfig (parser, logger);
            PropertyConfiguration configuration = new PropertyConfiguration (props);
            // patch extra config dir
            setDefaultValue (parser, configuration, "ext.conf.dir", 'e');
            // patch jmx enable settings
            setDefaultValue (parser, configuration, "jmx.enabled", 'X');

            context.putIfAbsent ("root", configuration);
            loadExtProperties (configuration, logger);
        } catch (Exception ex) {
            logger.warn (ex.getMessage (), ex);
            throw new RuntimeException (ex);
        }

        if (logger.isTraceEnabled ()) {
            logger.trace ("configurations load complete, trying to start application");
        }

        if (null != type) {

        }
    }

    private static void initLogger (ClassLoader loader, ArgumentParser parser) throws IOException {
        String logLevel, logFile;
        if (parser.isArgPresent ('v')) {
            logLevel = "TRACE";
        } else if (parser.isArgPresent ("log-level")) {
            logLevel = parser.getValue ("log-level");
        } else {
            logLevel = parser.getDefaultValue ("log-level");
        }

        logFile = parser.getValue ("log-file");
        if (StringUtil.isEmpty (logFile)) {
            logFile = parser.getDefaultValue ("log-file");
        }
        File file = new File (logFile);
        File parent = file.getParentFile ();
        if (!parent.exists () && !parent.mkdirs ()) {
            throw new IOException ("Can't create dir: " + parent.getCanonicalPath ());
        }

        try (InputStream in = loader.getResourceAsStream ("internal-log4j.properties")) {
            Properties props = new Properties ();
            props.load (in);

            System.out.println ("### setting log level to " + logLevel + " ###");
            if ("trace".equalsIgnoreCase (logLevel)) {
                props.setProperty ("log4j.rootLogger", "INFO, stdout, FILE");
                props.setProperty ("log4j.appender.FILE.File", logFile);
                props.setProperty ("log4j.appender.FILE.Threshold", logLevel);
                props.setProperty ("log4j.logger.org.dreamwork", "trace");
            } else {
                props.setProperty ("log4j.rootLogger", logLevel + ", stdout, FILE");
                props.setProperty ("log4j.appender.FILE.File", logFile);
                props.setProperty ("log4j.appender.FILE.Threshold", logLevel);
            }

            PropertyConfigurator.configure (props);
        }
    }

    private static Properties parseConfig (ArgumentParser parser, Logger logger) throws IOException {
        if (logger.isTraceEnabled ()) {
            logger.trace ("parsing config file ...");
        }
        String config_file = null;
        if (parser.isArgPresent ('c')) {
            config_file = parser.getValue ('c');
        }
        if (StringUtil.isEmpty (config_file)) {
            config_file = parser.getDefaultValue ('c');
        }

        config_file = config_file.trim ();
        if (logger.isTraceEnabled ()) {
            logger.trace ("config file: {}", config_file);
        }
        File file;
        if (config_file.startsWith ("file:/") || config_file.startsWith ("/")) {
            file = new File (config_file);
        } else {
            file = new File (".", config_file);
        }

        Properties props = new Properties ();
        if (!file.exists ()) {
            logger.warn ("can't find config file: {}", config_file);
            logger.warn ("using default config.");
        } else {
            try (InputStream in = new FileInputStream (file)) {
                props.load (in);
            }

            if (logger.isTraceEnabled ()) {
                prettyPrint (props, logger);
            }
        }
        return props;
    }

    private static void load (ClassLoader loader, Gson g, Map<String, Argument> map, String name) {
        try (InputStream in = loader.getResourceAsStream (name)) {
            if (in != null) {
                String content = new String (IOUtil.read (in), StandardCharsets.UTF_8);
                List<Argument> list = g.fromJson (content, Argument.AS_LIST);
                list.forEach (item -> {
                    String key = item.shortOption;
                    if (StringUtil.isEmpty (key)) {
                        key = item.longOption;
                    }
                    if (!StringUtil.isEmpty (key)) {
                        map.putIfAbsent (key, item);
                    }
                });
            }
        } catch (Exception ex) {
            throw new RuntimeException (ex);
        }
    }

    public static void prettyPrint (Properties props, Logger logger) {
        logger.trace ("### global configuration ###");
        int length = 0;
        List<String> list = new ArrayList<> ();
        for (String key : props.stringPropertyNames ()) {
            list.add (key);
            if (key.length () > length) {
                length = key.length ();
            }
        }
        list.sort (String::compareTo);
        for (String key : list) {
            StringBuilder builder = new StringBuilder (key);
            if (key.length () < length) {
                int d = length - key.length ();
                for (int i = 0; i < d; i ++) {
                    builder.append (' ');
                }
            }
            builder.append (" : ").append (props.getProperty (key));
            logger.trace (builder.toString ());
        }
        logger.trace ("############################");
    }

    private static void setDefaultValue (ArgumentParser parser, PropertyConfiguration configuration, String key, char argument) {
        if (parser.isArgPresent (argument)) {
            configuration.setRawProperty (key, parser.getValue (argument));
        }
        if (!configuration.contains (key)) {
            configuration.setRawProperty (key, parser.getDefaultValue (argument));
        }
    }

    private static void loadExtProperties (IConfiguration conf, Logger logger) {
        String ext_dir = conf.getString ("ext.conf.dir");
        if (!StringUtil.isEmpty (ext_dir)) {
            Path p = Paths.get (ext_dir);
            if (Files.exists (p)) {
                try {
                    Files.list (p).forEach (item -> {
                        String name = FileInfo.getFileNameWithoutExtension (item.toString ());
                        try (InputStream in = Files.newInputStream (item, StandardOpenOption.READ)) {
                            Properties props = new Properties ();
                            props.load (in);
                            context.putIfAbsent (name, new PropertyConfiguration (props));
                        } catch (IOException ex) {
                            logger.warn (ex.getMessage (), ex);
                        }
                    });
                } catch (IOException ex) {
                    logger.warn (ex.getMessage (), ex);
                }
            }
        }
    }
}
