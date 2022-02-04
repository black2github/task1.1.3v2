package jm.task.core.jdbc.dao;

import jm.task.core.jdbc.model.User;
import jm.task.core.jdbc.util.Util;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.Transaction;

import javax.persistence.EntityManager;
import javax.persistence.EntityTransaction;
import java.sql.SQLSyntaxErrorException;
import java.util.ArrayList;
import java.util.List;

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
        String sql = "CREATE TABLE Users ("
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
            if (isCauseContains(e, "already exists")) {
                // ignore
            } else {
                e.printStackTrace();
            }
            if (transaction != null) {
                //transaction.rollback();
            }
        }
    }

    @Override
    public void dropUsersTable() {

        Transaction transaction = null;
        try (Session session = factory.openSession()) {
            transaction = session.beginTransaction();
            session.createSQLQuery("DROP TABLE Users;").executeUpdate();
            transaction.commit();
        } catch (Exception e) {
            if (isCauseContains(e, "Unknown table")) {
                // ignore
            } else {
                e.printStackTrace();
            }
            if (transaction != null) {
                //transaction.rollback();
            }
        }
    }

    private static boolean isCauseContains(Throwable ex, String msg) {
        if (ex == null || msg == null || msg.isEmpty()) return false;
        if (ex.getMessage().indexOf(msg) != -1) {
            return true;
        } else {
            return isCauseContains(ex.getCause(), msg);
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
            User user = session.get(User.class, id);
            //session.remove(user);
            session.delete(user);
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
                em.remove(user);
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
}
