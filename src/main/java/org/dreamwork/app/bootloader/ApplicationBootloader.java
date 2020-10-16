package org.dreamwork.app.bootloader;

import com.google.gson.Gson;
import org.apache.log4j.PropertyConfigurator;
import org.dreamwork.cli.Argument;
import org.dreamwork.cli.ArgumentParser;
import org.dreamwork.config.IConfiguration;
import org.dreamwork.config.PropertyConfiguration;
import org.dreamwork.util.IOUtil;
import org.dreamwork.util.StringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class ApplicationBootloader {
    private static final Map<String, IConfiguration> context = new HashMap<> ();
    private static final Map<String, Lock> locks = new HashMap<> ();
    private static final Map<String, List<Object>> waiters = new HashMap<> ();

    private static ArgumentParser parser = null;

    public static boolean isArgPresent (String option) {
        return parser.isArgPresent (option);
    }

    public static boolean isArgPresent (char option) {
        return parser.isArgPresent (option);
    }

    public static IConfiguration getRootConfiguration () {
        return context.get ("root");
    }

    public static IConfiguration getConfiguration (String name) {
        synchronized (context) {
            if (context.containsKey (name)) {
                return context.get (name);
            }
        }

        Lock locker;
        synchronized (locks) {
            if (locks.containsKey (name)) {
                locker = locks.get (name);
            } else {
                locker = new ReentrantLock ();
                locks.put (name, locker);
            }
        }

        String label = Thread.currentThread ().getName ();
        do {
            System.out.printf ("[%s] acquiring a lock associated to %s%n", label, name);
            if (locker.tryLock ()) break;
            System.out.printf ("[%s] the locker is locked, wait for a moment%n", label);
            Object o;
            synchronized (waiters) {
                List<Object> list = waiters.computeIfAbsent (name, key -> new ArrayList<> ());
                list.add (o = new byte[0]);
            }
            synchronized (o) {
                try {
                    o.wait ();
                } catch (InterruptedException e) {
                    e.printStackTrace ();
                }
            }
        } while (true);

        try {
            // now, we got a active lock
            System.out.printf ("[%s] wo got a valid lock%n", label);
            synchronized (context) {
                // check the cache again, 'cause while we are waiting for the lock,
                // another thread might have loaded the configuration and stored it in the cache.
                if (context.containsKey (name)) {
                    return context.get (name);
                }
            }

            loadExtProperties (context.get ("root"), name);
            return context.get (name);
        } finally {
            // release the lock
            locker.unlock ();
            System.out.printf ("[%s] release the lock!%n", label);
            // release the waiting locker
            Object o = null;
            synchronized (waiters) {
                List<Object> list = waiters.get (name);
                if (list != null && !list.isEmpty ()) {
                    o = list.get (0);
                    list.remove (0);
                }
            }
            if (o != null) synchronized (o){
                o.notifyAll ();
            }
        }
    }

    public static void run (Class<?> type, String[] args) throws InvocationTargetException {
        ClassLoader loader = ApplicationBootloader.class.getClassLoader ();

        Map<String, Argument> map = new HashMap<> ();
        Gson g = new Gson ();

        load (loader, g, map, "application-bootloader.json");
        IBootable ib;
        if (type != null) {
            if (type.isAnnotationPresent (IBootable.class)) {
                ib = type.getAnnotation (IBootable.class);
                String argDef = ib.argumentDef ();
                if (StringUtil.isEmpty (argDef)) {
                    argDef = StringUtil.camelDecode (type.getName (), '-') + ".json";
                }

                load (loader, g, map, argDef);
            }
        }

        if (!map.isEmpty ()) {
            parser = new ArgumentParser (new ArrayList<> (map.values ()));
        }

        if (parser == null) {
            System.err.println ("can't initial command line parser");
            System.exit (-1);
            return;
        }

        parser.parse (args);

        if (parser.isArgPresent ('h')) {
            parser.showHelp ();
            System.exit (0);
        }

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

            Collection<Argument> ca = parser.getAllArguments ();
            for (Argument a : ca) {
                if (!StringUtil.isEmpty (a.propKey)) {
                    if (!StringUtil.isEmpty (a.shortOption))
                        setDefaultValue (parser, configuration, a.propKey, a.shortOption.charAt (0));
                    else if (!StringUtil.isEmpty (a.longOption))
                        setDefaultValue (parser, configuration, a.propKey, a.longOption);
                }
            }

/*
            // 所有配置文件解析完成，合并 System.getProperties 的属性
            Properties sys = System.getProperties ();
            for (String key : sys.stringPropertyNames ()) {
                configuration.setRawProperty (key, sys.getProperty (key));
            }

*/
            context.putIfAbsent ("root", configuration);
        } catch (Exception ex) {
            logger.warn (ex.getMessage (), ex);
            throw new RuntimeException (ex);
        }

        if (logger.isTraceEnabled ()) {
            logger.trace ("configurations load complete, trying to start application");
        }

        if (null != type) {
            Method[] methods = type.getMethods ();
            Method method = null;
            for (Method m : methods) {
                if (m.isAnnotationPresent (ApplicationEntrance.class)) {
                    method = m;
                    break;
                }
            }

            boolean has_args = false;
            if (method == null) {
                try {
                    method = type.getMethod ("start", IConfiguration.class);

                    if (method != null) {
                        try {
                            if (method.getModifiers () == Modifier.STATIC) {
                                method.invoke (null, getRootConfiguration ());
                            } else {
                                Object o = type.newInstance ();
                                method.invoke (o, getRootConfiguration ());
                            }
                        } catch (Exception ex) {
                            logger.warn (ex.getMessage (), ex);
                            throw new InvocationTargetException (ex);
                        }

                        return;
                    }
                } catch (NoSuchMethodException ex) {
//                    logger.warn (ex.getMessage (), ex);
                    if (logger.isTraceEnabled ()) {
                        logger.trace ("{} has no method named [start] take one argument::{}", type, IConfiguration.class);
                    }
                }
            }

            if (method == null) {
                try {
                    method = type.getMethod ("start", String[].class);
                    has_args = true;
                } catch (NoSuchMethodException ex) {
//                    ex.printStackTrace ();
                    if (logger.isTraceEnabled ()) {
                        logger.trace ("{} has no method named [start] take parameters::String[]", type);
                    }
                }
            }

            if (method == null) {
                try {
                    method = type.getMethod ("start");
                } catch (NoSuchMethodException ex) {
//                    ex.printStackTrace ();
                    if (logger.isTraceEnabled ()) {
                        logger.trace ("{} has no method named [start] without parameter", type);
                    }
                }
            }

            if (method != null) {
                try {
                    if (method.getModifiers () == Modifier.STATIC) {
                        if (has_args) {
                            method.invoke (null, new Object[] {args});
                        } else {
                            method.invoke (null);
                        }
                    } else {
                        Object o = type.newInstance ();
                        if (has_args) {
                            method.invoke (o, new Object[] {args});
                        } else {
                            method.invoke (o);
                        }
                    }
                } catch (Exception ex) {
                    throw new RuntimeException (ex);
                }
            } else {
                logger.error ("Can't find application entrance within type {}", type);
            }
        } else {
            logger.error ("Can't find entrance type!!");
            throw new IllegalArgumentException ("Can't find entrance type!!");
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

        if ("TRACE".equalsIgnoreCase (logLevel)) {
            System.out.printf ("## log file: %s ##%n", file.getCanonicalFile ());
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

            if (parser.isArgPresent ("trace-prefix")) {
                String prefixes = parser.getValue ("trace-prefix");
                if (!StringUtil.isEmpty (prefixes)) {
                    String[] parts = prefixes.trim ().split (File.pathSeparator);
                    for (String prefix : parts) {
                        if ("trace".equalsIgnoreCase (logLevel)) {
                            System.out.printf ("#### setting %s log level to trace ####%n", prefix);
                        }
                        props.setProperty ("log4j.logger." + prefix, "trace");
                    }
                }
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
                        map.put (key, item);
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

    private static void setDefaultValue (ArgumentParser parser, PropertyConfiguration configuration, String key, String argument) {
        if (parser.isArgPresent (argument)) {
            configuration.setRawProperty (key, parser.getValue (argument));
        }
        if (!configuration.contains (key)) {
            configuration.setRawProperty (key, parser.getDefaultValue (argument));
        }
    }

    private static void loadExtProperties (IConfiguration conf, String name) {
        String ext_dir = conf.getString ("ext.conf.dir");
        Path path = Paths.get (ext_dir, name + ".conf");
        if (Files.exists (path)) {
            try (InputStream in = Files.newInputStream (path, StandardOpenOption.READ)) {
                Properties props = new Properties ();
                props.load (in);
                context.putIfAbsent (name, new PropertyConfiguration (props));
            } catch (IOException ex) {
                ex.printStackTrace ();
            }
        }
    }

    public static void main (String[] args) throws InvocationTargetException {
        run (null, args);
/*
        final Map<String, Lock> locks = new HashMap<> ();
        final Map<String, List<Worker>> waiters = new HashMap<> ();
        final CountDownLatch latch = new CountDownLatch (10);
        ExecutorService executor = Executors.newCachedThreadPool ();

        for (int i = 0; i < 10; i ++) {
            int index = (int) (Math.random () * 3) + 1;
            final String name = "thread-" + index;
            Worker worker = new Worker (locks, waiters, name, i);
            worker.latch  = latch;
            executor.execute (worker);
        }
        executor.shutdown ();
        try {
            latch.await ();
        } catch (InterruptedException e) {
            e.printStackTrace ();
        }
        System.out.println (locks);
        System.out.println (waiters);
*/
    }

    private static final class Worker implements Runnable {
        final String name;
        final int counter;
        final Map<String, Lock> locks;
        final Map<String, List<Worker>> waiters;
        final Object LOCKER = new byte[0];
        CountDownLatch latch;

        Worker (Map<String, Lock> locks, Map<String, List<Worker>> waiters, String name, int counter) {
            this.waiters = waiters;
            this.locks   = locks;
            this.name    = name;
            this.counter = counter;
        }
        @Override
        public void run () {
            Lock locker;
            SimpleDateFormat sdf = new SimpleDateFormat ("yyyy-MM-dd HH:mm:ss");
            System.out.printf ("[%s][%s.%d] counter = %d%n", sdf.format (System.currentTimeMillis ()), name, counter, counter);
            synchronized (locks) {
                if (locks.containsKey (name)) {
                    locker = locks.get (name);
                    System.out.printf ("[%s][%s.%d] lock exists%n",sdf.format (System.currentTimeMillis ()), name, counter);
                } else {
                    locker = new ReentrantLock ();
                    locks.put (name, locker);
                    System.out.printf ("[%s][%s.%d] first round%n",sdf.format (System.currentTimeMillis ()), name, counter);
                }
            }

            boolean printed = false;
            do {
                System.out.printf ("[%s][%s.%d] acquiring a lock...%n", sdf.format (System.currentTimeMillis ()), name, counter);
                if (!locker.tryLock ()) {
                    System.out.printf ("[%s][%s.%d] the locker is locked. wait for a moment ...%n", sdf.format (System.currentTimeMillis ()), name, counter);
                    synchronized (waiters) {
                        List<Worker> list = waiters.computeIfAbsent (name, key -> new ArrayList<> ());
                        list.add (this);
                    }
                    try {
                        await ();
                    } catch (InterruptedException e) {
                        e.printStackTrace ();
                    }
                } else {
                    break;
                }
            } while (true);

            // now the lock is locked
            System.out.printf ("[%s][%s.%d] got the locker!%n", sdf.format (System.currentTimeMillis ()), name, counter);
            try {
                int time = (int) (Math.random () * 5) + 1;
                System.out.printf ("[%s][%s.%d] will waiting for %d seconds.%n", sdf.format (System.currentTimeMillis ()), name, counter, time);
                try {
                    Thread.sleep (time * 1000);
                } catch (Exception ex) {
                    ex.printStackTrace ();
                }
            } finally {
                locker.unlock ();
                Worker worker = null;
                synchronized (waiters) {
                    List<Worker> list = waiters.get (name);
                    if (list != null && !list.isEmpty ()) {
                        worker = list.get (0);
                        list.remove (0);
                    }
                }
                if (worker != null) {
                    worker.signal ();
                }
                System.out.printf ("[%s][%s.%d] unlocked%n", sdf.format (System.currentTimeMillis ()), name, counter);

                latch.countDown ();
            }
        }

        private void await () throws InterruptedException {
            synchronized (LOCKER) {
                LOCKER.wait ();
            }
        }

        private void signal () {
            synchronized (LOCKER) {
                LOCKER.notifyAll ();
            }
        }
    }
}