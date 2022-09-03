package jm.task.core.jdbc.util;

import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.boot.registry.StandardServiceRegistryBuilder;
import org.hibernate.cfg.Configuration;
import org.hibernate.cfg.Environment;
import org.hibernate.engine.spi.EntityEntry;
import org.hibernate.engine.spi.PersistenceContext;
import org.hibernate.engine.spi.SessionImplementor;
import org.hibernate.service.ServiceRegistry;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Map;
import java.util.Properties;

public class Util {
    // хранение настроек к СУБД в файле
    private static Properties properties = new Properties();
    static {
        try {
            InputStream in = Util.class.getResourceAsStream("/db.properties");
            properties.load(in);
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(-1);
        }
        for (String s : properties.stringPropertyNames()) {
            System.out.println(s + "=" + properties.getProperty(s));
        }
    }

    // реализуйте настройку соеденения с БД
    private static SessionFactory sessionFactory;

    public static SessionFactory getSessionFactory() {
        if (sessionFactory == null) {
            try {
                Configuration configuration = new Configuration();

                // Hibernate settings equivalent to hibernate.cfg.xml's properties
                Properties settings = new Properties();
                settings.put(Environment.DRIVER, properties.getProperty("driver"));
                String connectionURL = "jdbc:mysql://"
                        + properties.getProperty("hostName")
                        + ":" + properties.getProperty("port")
                        + "/" + properties.getProperty("dbName");
                settings.put(Environment.URL, connectionURL);
                settings.put(Environment.USER, properties.getProperty("userName"));
                settings.put(Environment.PASS, properties.getProperty("password"));
                settings.put(Environment.DIALECT, "org.hibernate.dialect.MySQL5Dialect");
                settings.put(Environment.SHOW_SQL, "true");
                settings.put(Environment.CURRENT_SESSION_CONTEXT_CLASS, "thread");
                //settings.put(Environment.HBM2DDL_AUTO, "create-drop");

                configuration.setProperties(settings);

                configuration.addAnnotatedClass(jm.task.core.jdbc.model.User.class);

                ServiceRegistry serviceRegistry = new StandardServiceRegistryBuilder()
                        .applySettings(configuration.getProperties()).build();

                sessionFactory = configuration.buildSessionFactory(serviceRegistry);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return sessionFactory;
    }

    /*
    Методы для JDBC вызовов
     */
    // Connect to MySQL
    public static Connection getConnection() throws SQLException {

        return getConnection(properties.getProperty("hostName"),
                properties.getProperty("dbName"),
                properties.getProperty("userName"),
                properties.getProperty("password")
        );
    }

    public static Connection getConnection(String hostName, String dbName,
                                           String userName, String password) throws SQLException {
        String connectionURL = "jdbc:mysql://"
                + properties.getProperty("hostName")
                + ":" + properties.getProperty("port")
                + "/" + properties.getProperty("dbName");
        Connection conn = DriverManager.getConnection(connectionURL,
                properties.getProperty("userName"),
                properties.getProperty("password")
        );
        return conn;
    }

    // возврат описания содержимого контекста для hibernate реализации
    public static String hibernateContextEntries(Session session, Object ... entities) {
        StringBuilder sb = new StringBuilder();
        SessionImplementor sessionSpi = session.unwrap(SessionImplementor.class);
        PersistenceContext pc = sessionSpi.getPersistenceContext();
        // entityEntries is an array of Map.Entry instances whose "key" is the entity itself and whose
        // value is an instance of org.hibernate.engine.spi.EntityEntry that describes various info about
        // the entity, including information like EntityEntry.getStatus().
        Map.Entry<Object, EntityEntry>[] entityEntries = pc.reentrantSafeEntityEntries();
        for (Map.Entry<Object, EntityEntry> e : entityEntries) {
            if (entities.length==0) {
                sb.append(toString(e)).append("\n");
            } else {
                for (int i=0; i < entities.length; i++) {
                    Object key = e.getKey();
                    if (entities[i].getClass()==key.getClass() && entities[i].equals(key)) {
                        sb.append(toString(e)).append("\n");
                    }
                }
            }
         }
        return sb.toString();
    }

    // формирование описания Map.Entry<Object, EntityEntry>
    private static String toString(Map.Entry<Object, EntityEntry> e) {
        StringBuilder sb = new StringBuilder();
        EntityEntry entry = e.getValue();
        sb.append("MapEntry={Key=").append(e.getKey())
                .append(" : Value=Entity[Id=").append(entry.getId())
                .append(", Status=").append(entry.getStatus())
                .append(", LockMode=").append(entry.getLockMode())
                .append(", Key=").append(entry.getEntityKey())
                .append(", Name=").append(entry.getEntityName())
                .append(", Version=").append(entry.getVersion())
                .append("]").append("}");
        return sb.toString();
    }
}
