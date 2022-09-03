package jm.task.core.jdbc.dao;

import jm.task.core.jdbc.model.User;
import jm.task.core.jdbc.util.Util;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.Transaction;
import org.hibernate.engine.spi.EntityEntry;
import org.hibernate.engine.spi.PersistenceContext;
import org.hibernate.engine.spi.SessionImplementor;
import org.hibernate.engine.spi.Status;

import javax.persistence.EntityManager;
import javax.persistence.EntityTransaction;
//import java.sql.SQLSyntaxErrorException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class UserDaoHibernateImpl implements UserDao {
    private SessionFactory factory = null;

    public UserDaoHibernateImpl() {
        try {
            factory = Util.getSessionFactory();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /*
    Методы создания и удаления таблицы пользователей в классе UserHibernateDaoImpl должны быть реализованы с
    помощью SQL.
     */
    @Override
    public void createUsersTable() {

        Transaction transaction = null;
        String sql = "CREATE TABLE IF NOT EXISTS Users ("
                + "id BIGINT NOT NULL AUTO_INCREMENT,"
                + "name VARCHAR(64) NULL,"
                + "lastName VARCHAR(64) NULL,"
                + "age TINYINT NULL,"
                + "PRIMARY KEY (`id`));";

        try (Session session = factory.openSession()) {
            transaction = session.beginTransaction();
            session.createSQLQuery(sql).executeUpdate();
            transaction.commit();
        } catch (Exception e) {
            e.printStackTrace();
            if (transaction != null) {
                transaction.rollback();
            }
        }
    }

    @Override
    public void dropUsersTable() {

        Transaction transaction = null;
        try (Session session = factory.openSession()) {
            transaction = session.beginTransaction();
            session.createSQLQuery("DROP TABLE IF EXISTS Users;").executeUpdate();
            transaction.commit();
        } catch (Exception e) {
            e.printStackTrace();
            if (transaction != null) {
                transaction.rollback();
            }
        }
    }

    @Override
    public void saveUser(String name, String lastName, byte age) {
        Transaction transaction = null;
        try (Session session = factory.openSession()) {
            transaction = session.beginTransaction();
            session.save(new User(name, lastName, age));
            transaction.commit();
        } catch (Exception e) {
            if (transaction != null) {
                transaction.rollback();
            }
        }
    }

    @Override
    public void removeUserById(long id) {
        Transaction transaction = null;

        try (Session session = factory.openSession()) {
            transaction = session.beginTransaction();
            User user = session.get(User.class, id); // Obtaining reference with its data initialized with Hibernate API
            //session.remove(user);
            session.delete(user); // Deleting an entity with the Hibernate API
            transaction.commit();
        } catch (Exception e) {
            e.printStackTrace();
            if (transaction != null) {
                transaction.rollback();
            }
        }
    }

    @Override
    public List<User> getAllUsers() {
        List<User> result = new ArrayList<>();
        try (Session session = factory.openSession()) {
            //result = session.createQuery( "from Users", User.class ).getResultList();
            result = (List<User>) session.createQuery("From User").list();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return result;
    }

    @Override
    public void cleanUsersTable() {
        EntityTransaction transaction = null;

        try (Session session = factory.openSession()) {
            EntityManager em = session.getEntityManagerFactory().createEntityManager();
            transaction = em.getTransaction();
            transaction.begin();
            List<User> list = getAllUsers();
            for (User user: list) {
                em.remove(user); // Deleting an entity with JPA
            }
            transaction.commit();
            em.close();
        } catch (Exception e) {
            e.printStackTrace();
            if (transaction != null) {
                transaction.rollback();
            }
        }
    }

    public void cleanUsersTable2() {
        Transaction transaction = null;

        try (Session session = factory.openSession()) {
            EntityManager em = session.getEntityManagerFactory().createEntityManager();
            transaction = session.beginTransaction();
            List<User> list = getAllUsers();
            for (User user: list) {
                em.remove(user); // Deleting an entity with JPA
            }
            transaction.commit();
            em.close();
        } catch (Exception e) {
            e.printStackTrace();
            if (transaction != null) {
                transaction.rollback();
            }
        }
    }

    @Override
    public void mixTest() {
        Transaction transaction = null;
        User user = null;
        try (Session session = factory.openSession()) {
            transaction = session.beginTransaction();
            user = new User("testName", "testLastName", (byte)123);
            session.save(user);
            System.out.println("after save():\n" + Util.hibernateContextEntries(session));
            transaction.commit();
            System.out.println("after commit():\n" + Util.hibernateContextEntries(session));
        } catch (Exception e) {
            if (transaction != null) {
                transaction.rollback();
            }
        }
        try {
            Session session = factory.openSession();
            transaction = session.beginTransaction();
            System.out.println("before merge():\n" + Util.hibernateContextEntries(session));
            user = (User) session.merge(user);
            System.out.println("after merge():\n" + Util.hibernateContextEntries(session, user));
            User user2 = new User("testName2", "testLastName2", (byte)10);
            session.persist(user2);
            System.out.println("after persist(user2):\n" + Util.hibernateContextEntries(session));
            User user3 = new User("testName3", "testLastName3", (byte)12);
            session.save(user3);
            System.out.println("after save(user3):\n" + Util.hibernateContextEntries(session));

            session.remove(user); // получит статус DELETED (для Hibernate), но остается в контектсе до его закрытия
            session.detach(user2); // будет удален из контектса
            System.out.println("after remove(user) and detach(user2):\n" + Util.hibernateContextEntries(session));
            user2.setLastName("lastName2_updated");
            session.merge(user2);
            System.out.println("after merge(user2):\n" + Util.hibernateContextEntries(session));
            transaction.commit();
            User userN = session.find(User.class, user2.getId());
            System.out.println(userN);
            System.out.println("before end:\n" + Util.hibernateContextEntries(session));
            session.close();

        } catch (Exception e) {
            e.printStackTrace();
        }
        System.out.println("mixTest end.");
    }
}
