package org.elasticsearch.river.jdbc;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import org.elasticsearch.common.xcontent.XContentBuilder;

public class ElasticTest {
    
    public static void main(String[] argv) {
    	try {
            String driverClassName = "com.microsoft.sqlserver.jdbc.SQLServerDriver";
            String url = "jdbc:sqlserver://184.106.106.239:1433;instanceName=dbo;database=DBCrawler;user=spideruser;password=tony6472rene91970;";
            String username = "spideruser";
            String password = "tony6472rene91970";
            String sql = "select TBLBaseRSSItems.intID, TBLBaseRSSItems.bitStaticWritten, TBLBaseRSSItems.strRSSItemLinkURL, TBLBaseRSSItems.strRSSItemTitle, TBLBaseRSSItems.strRSSItemKeywords, TBLBaseRSSItems.dtRSSItemDateTime, TBLBaseRSSItems.strRSSItemDesc, TBLBaseRSSItems.intFKstrRssLinkURL, TBLBaseRSSItems.strRSSSemItemKeywords, TBLBaseRSSItems.strRSSItemTitleKeywords, TBLBaseRSSItems.bitHasLocation, TBLBaseRSSItems.strOpinon, TBLBaseRSSItems.strObjSubj, TBLBaseRSSItems.strSGML, TBLBaseRSSItems.strLocation, TBLBaseRSSItems.strNames, TBLBaseRSSItems.geoLat, TBLBaseRSSItems.geoLong, TBLBaseRSS.strURLType from TBLBaseRSSItems, TBLBaseRSS WHERE TBLBaseRSSItems.intFKstrRSSLinkURL = TBLBaseRSS.intID AND TBLBaseRSSItems.bitStaticWritten = 0; ";
            List<Object> params = new ArrayList();
            int fetchsize = 0;
            MergeListener listener = new MergeListener() {

                @Override
                public void merged(String index, String type, String id, XContentBuilder builder) throws IOException {
                    System.err.println("index=" + index + " type=" + type + " id=" + id + " builder=" + builder.string());
                }
            };
            SQLService service = new SQLService();
            Connection connection = service.getConnection(driverClassName, url, username, password);
            PreparedStatement statement = service.prepareStatement(connection, sql);
            service.bind(statement, params);
            ResultSet results = service.execute(statement, fetchsize);
            Merger merger = new Merger(listener);
            long rows = 0L;
            while (service.nextRow(results, merger)) {
                rows++;
            }
            merger.close();
            System.err.println("rows = " + rows);
            service.close(results);
            service.close(statement);
            service.close(connection);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
