import java.io.FileWriter;
import java.sql.*;
import java.util.LinkedHashMap;

public class H2jdbcToFile {
    // Название JDBC драйвера и URL базы данных
    static final String JDBC_DRIVER = "org.h2.Driver";
    static final String DB_URL = "jdbc:h2:~/test";

    // Информация JDBC пользователя
    static final String USER = "sa";
    static final String PASS = "";

    public static void main(String[] args) throws Exception {
        Connection conn = null;
        Statement stmt = null;
        try{
            // Регистрация JDBC драйвера
            Class.forName(JDBC_DRIVER);

            // Открытие подключения
            System.out.println("Подключение к базе данных...");
            conn = DriverManager.getConnection(DB_URL,USER, PASS);

            // Открываем Statement для запроса
            System.out.println("Открываем Statement для запроса...");
            stmt = conn.createStatement();

            // Считывание информации таблицы TABLE_LIST в ResultSet
            ResultSet rs1 = stmt.executeQuery("select * from TABLE_LIST");

            // Создание HashMap для таблицы TABLE_LIST
            LinkedHashMap<String,String> table_list = new LinkedHashMap<>();

            // Заполнение HashMap с помощью ResultSet
            System.out.println("Заполнение HashMap для таблицы TABLE_LIST");
            while (rs1.next()){
                String str = rs1.getString("PK");
                String[] keys = str.split(",");
                for (String key:keys){
                    table_list.put(key.trim(), rs1.getString("TABLE_NAME"));
                }
            }
            System.out.println("HashMap для таблицы TABLE_LIST:" + "\n" + table_list);

            // Получение результата 2 запроса
            ResultSet rs2 = stmt.executeQuery("select * from TABLE_COLS");

            // Создание LinkedHashMap для Result Set
            LinkedHashMap<String, String> table_cols = new LinkedHashMap<>();

            // Заполнение LinkedHashMap с помощью ResultSet
            System.out.println("Заполнение HashMap для таблицы TABLE_COLS");
            while (rs2.next()){
                table_cols.put(rs2.getString("COLUMN_NAME"), rs2.getString("COLUMN_TYPE"));
            }
            System.out.println("HashMap для таблицы TABLE_COLS:" + "\n" + table_cols);

            // Файл для записи
            FileWriter writer = new FileWriter("file.txt");
            // Сравниваем keySet'ы LinkedHashMap и записываем в файл
            for (String key1 : table_list.keySet()) {
                for (String key2 : table_cols.keySet()) {
                    if (key1.equalsIgnoreCase(key2)) {
                        String str = (table_list.get(key1) + ", " + key1 + ", " + table_cols.get(key2) +'\n' );
                        writer.write(str);
                    }
                }
            }
            writer.close();

            // Закрытие окружения
            stmt.close();
            conn.close();
            System.out.println("Отключение от базы данных");
        }catch (SQLException se) {
            // Обработка ошибок для JDBC
            se.printStackTrace();
        } catch (Exception e) {
            // Обработка ошибок for Class.forName
            e.printStackTrace();
        } finally {
            // Закрытие ресурсов
            try {
                if (stmt!=null) stmt.close();
            } catch (SQLException se2) { }
            try {
                if (conn!=null) conn.close();
            }catch (SQLException se){
                se.printStackTrace();
            }
        } // end try
        System.out.println("Конец");
    }
}