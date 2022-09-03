package jm.task.core.jdbc;

import jm.task.core.jdbc.model.User;
import jm.task.core.jdbc.service.UserService;
import jm.task.core.jdbc.service.UserServiceImpl;
import jm.task.core.jdbc.util.Util;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

public class Main {
    public static void main(String[] args) throws SQLException, ClassNotFoundException {
        // реализуйте алгоритм здесь
        UserService userService = new UserServiceImpl();

        // Создание таблицы User(ов)
        userService.createUsersTable();

        // Добавление 4 User(ов) в таблицу с данными на свой выбор. После каждого добавления должен быть вывод в
        // консоль ( User с именем – name добавлен в базу данных )
        for (int i=0; i < 4; i++ ) {
            userService.saveUser("name"+i, "lastName"+i, (byte)i);
            System.out.printf("User с именем - %s добавлен в базу данных\n", "name"+i);
        }

        // Получение всех User из базы и вывод в консоль ( должен быть переопределен toString в классе User)
        List<User> list = userService.getAllUsers();
        for (User user : list) {
            System.out.println(user);
        }

        // Очистка таблицы User(ов)
        userService.cleanUsersTable();

        // разные тесты для проверки
        userService.mixTest();

        // Удаление таблицы
        userService.dropUsersTable();
    }
}
