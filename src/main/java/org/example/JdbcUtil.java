package org.example;

import com.google.common.base.CaseFormat;
import lombok.NoArgsConstructor;
import org.apache.commons.beanutils.BeanUtils;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.ArrayList;
import java.util.List;

public class JdbcUtil {
    public static <T> List<T> queryList(Connection connection, String querySql, Class<T> clz, boolean underScoreToCamel) throws Exception {
        ArrayList<T> resultList = new ArrayList<>();
        try (PreparedStatement preparedStatement = connection.prepareStatement(querySql)) {
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                ResultSetMetaData metaData = resultSet.getMetaData();
                int columnCount = metaData.getColumnCount();
                while (resultSet.next()) {
                    T t = clz.newInstance();
                    for (int i = 1; i < columnCount + 1; i++) {
                        String columnName = metaData.getColumnName(i);
                        if (underScoreToCamel) {
                            columnName = CaseFormat.LOWER_UNDERSCORE
                                    .to(CaseFormat.LOWER_CAMEL, columnName.toLowerCase());
                        }
                        Object value = resultSet.getObject(i);
                        BeanUtils.setProperty(t, columnName, value);
                    }
                    resultList.add(t);
                }
            }
        }

        return resultList;
    }
}
