package com.fastcampus.flink;

import org.apache.flink.connector.datagen.table.DataGenConnectorOptions;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableDescriptor;
import org.apache.flink.table.api.TableEnvironment;



public class TableApiExample {
    public static void main(String[] args) {
        EnvironmentSettings settings = EnvironmentSettings.newInstance()// EnvironmentSettings를 사용하여 스트리밍 모드로 Flink 환경을 설정합니다.
            .inStreamingMode() // 스트리밍 모드에서 데이터를 실시간으로 처리하려고 설정(Flink는 기본적으로 배치 처리와 스트리밍 처리를 지원)
            .build();
        
        // TableEnvironment는 Flink에서 SQL 및 Table API를 실행할 수 있는 환경을 설정하는 객체입니다. tableEnv 객체는 이를 통해 SQL 쿼리나 테이블 API 작업을 수행
        TableEnvironment tableEnv = TableEnvironment.create(settings);

        tableEnv.createTemporaryTable("SourceTable",  // SourceTable은 datagen 커넥터를 사용하여 데이터를 임의로 생성하는 테이블
            TableDescriptor.forConnector("datagen") // datagen 커넥터를 사용하여 데이터를 생성합니다. 이 커넥터는 Flink에서 테스트나 개발 환경에서 임의의 데이터를 생성하는 데 사용
                .schema(Schema.newBuilder() // 이 부분에서는 SourceTable에 정의된 열들의 스키마를 정의
                        .column("f0", DataTypes.STRING()) // STRING 타입의 열
                        .column("f1", DataTypes.BIGINT()) // BIGINT 타입의 열
                        .build())
                .option(DataGenConnectorOptions.ROWS_PER_SECOND, 1L) // 이 설정은 초당 1개의 행을 생성하도록 지정합니다. datagen 커넥터는 데이터를 주기적으로 생성합니다.
                .build()
            );

        tableEnv.createTemporaryTable("SinkTable",  // SinkTable은 print 커넥터를 사용하여 데이터를 표준 출력에 출력하는 테이블
            TableDescriptor.forConnector("print") // print 커넥터는 데이터를 출력하는 데 사용됩니다. 이 경우, 데이터를 콘솔(표준 출력)에 출력합니다.
                .schema(Schema.newBuilder() // SourceTable과 동일한 구조를 가진 열을 정의합니다. 이 테이블은 SourceTable에서 생성된 데이터를 그대로 출력할 수 있습니다.
                        .column("f0", DataTypes.STRING())
                        .column("f1", DataTypes.BIGINT())
                        .build())
                .build()
            );

        Table table1 = tableEnv.from("SourceTable"); // SourceTable을 읽어들여 table1 객체로 만듭니다.

        Table table2 = tableEnv.sqlQuery("SELECT * FROM SourceTable"); //  SQL 쿼리를 사용하여 SourceTable에서 데이터를 읽어옵니다


        // table2에서 읽어온 데이터를 SinkTable로 출력합니다. SinkTable은 print 커넥터를 사용하므로, 이 데이터는 콘솔에 출력됩니다.
        // .execute()는 쿼리를 실행하는 명령입니다. 이 메서드는 데이터 흐름을 시작하고, SourceTable에서 SinkTable로 데이터를 전송하게 됩니다.
        table2.insertInto("SinkTable").execute(); 
    } 
}
// datagen 커넥터는 초당 1개의 행을 생성하므로, 이 데이터는 실시간으로 SourceTable에서 생성되어 SinkTable로 출력됩니다.
// SinkTable은 print 커넥터를 사용하므로 데이터를 콘솔에 출력합니다.