package Experiment;

import java.io.IOException;
import java.util.logging.FileHandler;
import java.util.logging.Handler;
import java.util.logging.Logger;

public class LoggerTest {
    public void test1() throws IOException {
        Logger logger = Logger.getLogger("LYK");

        // Assuming the default logging.properties file only uses ConsoleHandler,
        // this is how I choose to use FileHandler at run-time.
        // One thing I don't get is that if the default logging.properties file
        // chooses to use ConsoleHandler, and if I add a FileHandler, shouldn't I get 2
        // handlers when I call logger.getHandlers()? But I only get 1.
        logger.addHandler(new FileHandler());

        logger.info("lyk test 1");
        System.out.println(getClass().getClassLoader().getResource("logging.properties"));
        // this prints null unless a "logging.properties" file is placed in any directory of the classpath.

        Handler[]  handlers = logger.getHandlers();
        System.out.println(handlers.length);
        System.out.println(handlers[0]);
    }
}
